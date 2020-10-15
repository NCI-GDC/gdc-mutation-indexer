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


@pytest.mark.usefixtures('maf_df', 'gistic_df')
class TestDFBuildersBase:
    """
    Code to reuse throughout all tests in this file
    """

    unique_fields = {
        'transcript': ['consequence_type'],
        'ssm': ['chromosome'],
        'cnv': ['chromosome', 'cnv_change'],
        'gene': ['biotype'],
        'annotation': ['vep_impact']
    }

    @pytest.fixture(scope='function')
    def get_inputs(self, maf_df, gistic_df, request):

        # get parameter values from the test using this fixture:
        index_type = request.getfixturevalue('index_type')
        df_type = request.getfixturevalue('df_type')

        # return corresponding get_function, input_df and id_field
        get_function = globals()['get_{}_df'.format(df_type)]

        extra_inputs = {}
        if 'cnv' in index_type or df_type == 'cnv':
            input_df = gistic_df
            extra_inputs['fields_to_delete'] = ['cnv_id', 'cnv_change']
        else:
            input_df = maf_df
            extra_inputs['fields_to_delete'] = ['ssm_id', 'mutation_subtype']

        extra_inputs['id_field'] = {
            'annotation': 'transcript_id',
            'transcript': 'transcript_id',
            'gene': 'gene_id',
            'ssm': 'ssm_id',
            'cnv': 'cnv_id',
        }[df_type]

        return get_function, input_df, extra_inputs

    @staticmethod
    def params():
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

    @classmethod
    def is_sub(cls, subset, superset, mapping=None):
        result = True
        for item in subset.items():
            (key, val) = item
            if type(val) is dict:
                result = result and cls.is_sub(val, superset, mapping)
            elif item not in superset:
                if key in mapping and val is not None:
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
        filtered_dict = {}
        filtered_list = df.filter(col(join_by) == item[join_by]).collect()
        for it in filtered_list:
            filtered_dict.update(it.asDict(recursive=True))
        assert cls.is_sub(item, filtered_dict.items(), mapping)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestDFBuilders(TestDFBuildersBase):
    @pytest.mark.parametrize('index_type,df_type', TestDFBuildersBase.params())
    def test_simple_df_build(self, index_type, df_type, get_inputs):
        """
        Attempts to build each dataframe from df_builders for each index
        Validates top level keys to match to mapping
        """
        get_function, input_df, extra_inputs = get_inputs
        id_field = extra_inputs['id_field']

        # the input_df has entries with duplicated id but different values
        input_df = input_df.drop_duplicates(subset=[id_field])

        df = get_function(input_df, index_type)
        mapping = select_mapping(index_type, df_type)['properties']

        # Do not check for unwanted keys
        stopwords = ['copy_to', '_autocomplete', 'gene_aa_change']
        must_have_keys = [
            k for k in mapping.keys()
            if all([-1 == k.find(s) for s in stopwords])
        ]

        # make sure all required keys exist
        assert set(df.columns) == set(must_have_keys)

        # make sure that values came from input_df
        self.assert_from_df(input_df, df.first(), id_field, mapping=mapping)

    @pytest.mark.parametrize('index_type,df_type', TestDFBuildersBase.params())
    def test_df_drop_fields(self, index_type, df_type, get_inputs):

        get_function, input_df, extra_inputs = get_inputs

        fields_to_delete = extra_inputs['fields_to_delete']

        df = get_function(
            input_df, index_type, drop_fields=fields_to_delete
        )

        for field in fields_to_delete:
            assert field not in df.columns

    @pytest.mark.parametrize('index_type,df_type', TestDFBuildersBase.params())
    def test_unique_fields(self, index_type, df_type, get_inputs):
        get_function, input_df, _ = get_inputs
        df = get_function(input_df, index_type, unique_fields=None)
        df_unique = get_function(input_df, index_type,
                                 unique_fields=self.unique_fields[df_type])
        assert df_unique.count() == (df.select(self.unique_fields[df_type])
                                       .distinct().count())

    @pytest.mark.parametrize('index_type,df_type', TestDFBuildersBase.params())
    def test_df_add_fields(self, df_type, index_type, get_inputs):
        get_function, input_df, _ = get_inputs
        df = get_function(input_df, index_type, add_fields=['case_id'])
        assert 'case_id' in df.columns
