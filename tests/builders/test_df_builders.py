import pytest
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import UserDefinedFunction, col

from exports.builders.utils import select_mapping
from exports.builders.consequence import ConsequenceBuilder
from exports.builders.df_builders import (
    get_annotation_df,
    get_gene_df,
    get_ssm_df,
    get_transcript_df,
)
from tests_config import TestConfig
conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestDFBuilders:

    @classmethod
    def is_sub(cls, subset, superset, mapping=None):

        result = True
        for item in subset.items():
            (key, val) = item
            if type(val) is dict:
                result = result and cls.is_sub(val, superset)
            elif item not in superset:
                if val is not None:
                    if (mapping[key].get('default'), val) in superset:
                        continue
                    print 'item not in superset:', item
                    result = False
                else:
                    print 'item not mapped', item

        return result

    @classmethod
    def assert_from_maf(cls, maf_df, row, join_by, mapping=None):
        item = row.asDict(recursive=True)
        maf = maf_df.filter(
            col(join_by) == item[join_by]).first().asDict(recursive=True)
        assert cls.is_sub(item, maf.items(), mapping)

    @pytest.mark.parametrize('df_type', ['transcript', 'ssm', 'gene', 'annotation'])
    @pytest.mark.parametrize('index_type', conf.indices.keys())
    def test_simple_df_build(self, maf_df, index_type, df_type):
        """
        Attempts to build each dataframe from df_builders for each index
        Validates top level keys to match to mapping
        """
        df = globals()['get_{}_df'.format(df_type)](maf_df, index_type)
        mapping = select_mapping(index_type, df_type)['properties']
        # Do not check for unwanted keys
        stopwords = ['copy_to', '_autocomplete', 'gene_aa_change']
        must_have_keys = [k for k in mapping.keys()
                          if all([-1 == k.find(s) for s in stopwords])]
        assert set(df.columns) == set(must_have_keys)

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_gene_df(self, maf_df, index_name):
        # convert is_cancer_gene_census to True as our test self.mafs aren't
        # in the census list
        if index_name != 'gene_centric':
            udf = UserDefinedFunction(lambda x: True, BooleanType())
            new_df = maf_df.withColumn('is_cancer_gene_census',
                                       udf(maf_df.is_cancer_gene_census))
            gene_df = get_gene_df(new_df, index_name)
            self.assert_from_maf(new_df, gene_df.first(), 'gene_id')

    @pytest.mark.parametrize('df_type', ['transcript', 'ssm', 'gene', 'annotation'])
    @pytest.mark.parametrize('index_type', conf.indices.keys())
    def test_df_drop_fields(self, maf_df, df_type, index_type):
        df = globals()['get_{}_df'.format(df_type)](
            maf_df, index_type, drop_fields=['ssm_id', 'mutation_subtype']
        )
        assert 'ssm_id' not in df.columns
        assert 'mutation_subtype' not in df.columns

    @pytest.mark.parametrize('df_type', ['transcript', 'ssm', 'gene', 'annotation'])
    @pytest.mark.parametrize('index_type', conf.indices.keys())
    def test_unique_fields(self, maf_df, df_type, index_type):
        unique_fields = {
            'transcript': ['consequence_type'],
            'ssm': ['chromosome'],
            'gene': ['biotype'],
            'annotation': ['vep_impact']
        }
        get_df = globals()['get_{}_df'.format(df_type)]

        df = get_df(maf_df, index_type, unique_fields=None)
        df_unique = get_df(maf_df, index_type,
                           unique_fields=unique_fields[df_type])
        assert df_unique.count() == (df.select(unique_fields[df_type])
                                       .distinct().count())

    @pytest.mark.parametrize('df_type', ['transcript', 'ssm', 'gene', 'annotation'])
    @pytest.mark.parametrize('index_type', conf.indices.keys())
    def test_df_add_fields(self, maf_df, df_type, index_type):
        df = globals()['get_{}_df'.format(df_type)](maf_df, index_type,
                                                    add_fields=['case_id'])
        assert 'case_id' in df.columns

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_annotation_df(self, sqlContext, maf_df, index_name):

        builder = ConsequenceBuilder(conf, sqlContext)
        exploded = builder.build_all_effects_cols(maf_df)
        ann_df = get_annotation_df(exploded, index_name, add_fields=['ssm_id'],
                                   unique_fields=['ssm_id', 'transcript_id'])
        ann_mapping = select_mapping(index_name, 'annotation')

        self.assert_from_maf(exploded, ann_df.first(), 'transcript_id',
                             mapping=ann_mapping['properties'])

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_ssm_df(self, maf_df, index_name):
        if index_name != 'ssm_centric':
            ssm_df = get_ssm_df(maf_df, index_name)
            self.assert_from_maf(maf_df, ssm_df.first(), 'ssm_id')

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_transcript_df(self, sqlContext, maf_df, index_name):
        builder = ConsequenceBuilder(conf, sqlContext)
        exploded = builder.build_all_effects_cols(maf_df)
        transcript = get_transcript_df(exploded, index_name)

        transcript_mapping = select_mapping(index_name, 'transcript')
        self.assert_from_maf(exploded, transcript.first(),
                             'transcript_id',
                             mapping=transcript_mapping['properties'])
