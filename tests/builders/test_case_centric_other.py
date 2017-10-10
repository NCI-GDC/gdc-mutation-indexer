import pytest

from exports.mappers.models_mapper import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('case_centric_df')
class TestCaseCentricOther:
    """ Test intermediatekresult from the case centric builder """

    def test_available_variation_data(self, test_index, case_centric_df):
        """ Test that cases without any ssm, but were tested are flagged """

        # Insert a case to graph with no data
        test_index.index(conf.graph_index, doc_type='case',
                         id='empty_case', body={'case_id': 'empty_case'})
        # Force ES to refresh before trying to build index
        test_index.indices.refresh(index=conf.graph_index)

        assert 'available_variation_data' in case_centric_df.columns
        # Sum of booleans, True = 1, False = 0, should only have one test case

        assert sum(
            [r['available_variation_data'] == ['ssm']
             for r in case_centric_df.select('available_variation_data').collect()]
        ) == case_centric_df.count() - 1

        assert sum([r['available_variation_data'] == [] for r in
                   case_centric_df.select('available_variation_data').collect()]) == 1

        assert (case_centric_df.cache()
                       .filter(case_centric_df.case_id == 'empty_case')
                       .select('available_variation_data')
                       .collect()[0]['available_variation_data'] == [])

        # Get rid of the test document and force an ES refresh
        test_index.delete(conf.graph_index, doc_type='case', id='empty_case')
        test_index.indices.refresh(index=conf.graph_index)

    @pytest.mark.parametrize('path', [
                             'case_id',
                             'available_variation_data',
                             'gene',
                             'gene.ssm',
                             ])
    def test_case_centric_path_exists(self, case_centric_df, path):
        """
        Chosen paths that have to be present to merge branch
        """
        case_centric_df.select(path)

    @pytest.mark.parametrize('path', ModelMapper('case_centric').get_paths())
    @pytest.mark.skipif(conf.skip_in_depth_tests,
                        reason='we want to merge partial data fixes.'\
                        'This test is used for missing fields lookup.')
    def test_all_paths_case(self, case_centric_df, path):
        """
        Check for existence of all paths that are in mapping
        Can be skipped with skip_id_depth_tests switch
        """
        case_centric_df.select(path)

