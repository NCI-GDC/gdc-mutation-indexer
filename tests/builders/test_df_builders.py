from pyspark.sql.functions import UserDefinedFunction, col
import pytest
from pyspark.sql.types import BooleanType

from exports.builders.utils import select_mapping
from exports.builders.consequence import ConsequenceBuilder
from exports.builders.df_builders import (
    get_annotation_df,
    get_gene_df,
    get_ssm_df,
    get_transcript_df,
    get_single_df
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

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_df_drop_fields(self, maf_df, index_name):
        df = get_single_df(maf_df, index_name, 'annotation',
                           drop_fields=['ssm_id', 'mutation_subtype'])
        assert 'ssm_id' not in df.columns
        assert 'mutation_subtype' not in df.columns

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_unique_fields(self, maf_df, index_name):
        ann_df = get_annotation_df(maf_df, index_name, unique_fields=['impact'])
        assert ann_df.count() < maf_df.count()

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_df_add_fields(self, maf_df, index_name):
        df = get_single_df(maf_df, index_name, 'annotation',
                           add_fields=['case_id'])
        assert 'case_id' in df.columns

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_annotation_df(self, maf_df, index_name):
        ann_df = get_annotation_df(maf_df, index_name)
        annotation = ann_df.first()
        self.assert_from_maf(maf_df, annotation, 'transcript_id')

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_ssm_df(self, maf_df, index_name):
        if index_name != 'ssm_centric':
            ssm_df = get_ssm_df(maf_df, index_name)
            self.assert_from_maf(maf_df, ssm_df.first(), 'ssm_id')

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_transcript_df(self, sqlContext, maf_df, index_name):
        builder = ConsequenceBuilder(conf, sqlContext)
        exploded = builder._build_all_effects_cols(maf_df)
        transcript = get_transcript_df(exploded, index_name)

        transcript_mapping = select_mapping(index_name, 'transcript')
        self.assert_from_maf(exploded, transcript.first(),
                             'transcript_id',
                             mapping=transcript_mapping['properties'])
