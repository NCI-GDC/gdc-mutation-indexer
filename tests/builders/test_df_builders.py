import itertools
import pytest
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import UserDefinedFunction, col

from exports.builders.utils import select_mapping
from exports.builders.consequence import ConsequenceBuilder
from exports.builders.df_builders import (
    get_annotation_df,
    get_gene_df,
    get_ssm_df,
    get_cnv_df,
    get_transcript_df,
)
from tests_config import TestConfig
conf = TestConfig()


def get_index_df_type_pairs():
    return itertools.chain(
        itertools.product(
            conf.main_indices,
            ['transcript', 'cnv', 'ssm', 'gene', 'annotation']
        ),
        itertools.product(
            conf.ssm_indices,
            ['transcript', 'ssm', 'gene', 'annotation']
        ),
        itertools.product(
            conf.cnv_indices,
            ['cnv', 'gene']
        )
    )


ID_FIELDS = {
    'annotation': 'transcript_id',
    'transcript': 'transcript_id',
    'gene': 'gene_id',
    'ssm': 'ssm_id',
    'cnv': 'cnv_id',
}


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
    def assert_from_df(cls, df, row, join_by, mapping=None):
        item = row.asDict(recursive=True)
        df = df.filter(col(join_by) == item[join_by]).first().asDict(recursive=True)
        assert cls.is_sub(item, df.items(), mapping)

    @pytest.mark.parametrize('index_type,df_type', get_index_df_type_pairs())
    def test_simple_df_build(self, maf_df, gistic_df, index_type, df_type):
        """
        Attempts to build each dataframe from df_builders for each index
        Validates top level keys to match to mapping
        """
        if 'cnv' in index_type or df_type == 'cnv':
            input_df = gistic_df
        else:
            input_df = maf_df

        df = globals()['get_{}_df'.format(df_type)](input_df, index_type)
        mapping = select_mapping(index_type, df_type)['properties']
        # Do not check for unwanted keys
        stopwords = ['copy_to', '_autocomplete', 'gene_aa_change']
        must_have_keys = [k for k in mapping.keys()
                          if all([-1 == k.find(s) for s in stopwords])]

        # make sure all required keys exist
        assert set(df.columns) == set(must_have_keys)

        # make sure that values came from input_df
        self.assert_from_df(input_df, df.first(), ID_FIELDS[df_type], mapping=mapping)

    @pytest.mark.parametrize('index_type,df_type', get_index_df_type_pairs())
    def test_df_drop_fields(self, maf_df, gistic_df, index_type, df_type):
        if 'cnv' in index_type or df_type == 'cnv':
            input_df = gistic_df
            fields_to_delete = ['cnv_id', 'cnv_change']
        else:
            input_df = maf_df
            fields_to_delete = ['ssm_id', 'mutation_subtype']
        df = globals()['get_{}_df'.format(df_type)](
            input_df, index_type, drop_fields=fields_to_delete
        )

        for field in fields_to_delete:
            assert field not in df.columns

    @pytest.mark.parametrize('index_type,df_type', get_index_df_type_pairs())
    def test_unique_fields(self, maf_df, gistic_df, index_type, df_type):

        if 'cnv' in index_type or df_type == 'cnv':
            input_df = gistic_df
        else:
            input_df = maf_df

        unique_fields = {
            'transcript': ['consequence_type'],
            'ssm': ['chromosome'],
            'cnv': ['chromosome', 'cnv_change'],
            'gene': ['biotype'],
            'annotation': ['vep_impact']
        }

        get_df = globals()['get_{}_df'.format(df_type)]

        df = get_df(input_df, index_type, unique_fields=None)
        df_unique = get_df(input_df, index_type,
                           unique_fields=unique_fields[df_type])
        assert df_unique.count() == (df.select(unique_fields[df_type])
                                       .distinct().count())

    @pytest.mark.parametrize('index_type,df_type', get_index_df_type_pairs())
    def test_df_add_fields(self, maf_df, gistic_df, df_type, index_type):
        if 'cnv' in index_type or df_type == 'cnv':
            input_df = gistic_df
        else:
            input_df = maf_df

        df = globals()['get_{}_df'.format(df_type)](input_df, index_type,
                                                    add_fields=['case_id'])
        assert 'case_id' in df.columns

