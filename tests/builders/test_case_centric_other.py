import pytest
import time

from exports.mappers.models_mapper import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('case_centric_df', 'es_client')
class TestCaseCentricOther:
    """ Test intermediatekresult from the case centric builder """

    def test_available_variation_data(self, es_client, case_centric_df):
        """ Test that cases without any ssm, but were tested are flagged """
        def wait_for_doc(es, index, doc_type, did, mode):
            assert mode in ['create', 'delete']
            while True:
                time.sleep(1)
                try:
                    es.get(index=index, doc_type=doc_type, id=did)
                    if mode == 'create':
                        return
                except:
                    if mode == 'delete':
                        return

        # Insert a case to graph with no data
        es_client.index(conf.graph_index, doc_type='case',
                        id='empty_case', body={'case_id': 'empty_case'})

        # Wait for document creation
        wait_for_doc(es_client, conf.graph_index, 'case', 'empty_case', 'create')

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

        # Get rid of the test document
        es_client.delete(conf.graph_index, doc_type='case', id='empty_case')
        
        # Wait for document deletion
        wait_for_doc(es_client, conf.graph_index, 'case', 'empty_case', 'delete')

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

