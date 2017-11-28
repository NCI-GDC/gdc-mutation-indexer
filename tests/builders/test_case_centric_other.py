import pytest

from exports.mappers.models_mapper import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'all_cases', 'case_centric_df')
class TestCaseCentricOther:
    """ Other case centric tests """

    def test_empty_cases(self, sqlContext, case_centric_df, maf_df, all_cases):
        """
        Test that cases without any ssm but were tested (aka "empty cases") are built and flagged
        Expected case set is retrieved from maf header's aliquot.sample_id-s
        """

        non_empty_cases = [r.case_id for r in maf_df.select('case_id').collect()]

        empty_cases = set(all_cases) - set(non_empty_cases)
        n_empty_cases = len(empty_cases)

        # There supposed to be some empty cases in test data
        assert n_empty_cases > 0

        assert 'available_variation_data' in case_centric_df.columns

        # Sum of booleans, True = 1, False = 0, should only have one test case
        df = (case_centric_df.select('case_id', 'available_variation_data')
                             .collect())

        # Check that all cases == built cases
        assert set(all_cases) == set([r.case_id for r in df])

        # Check that for empty cases 'available_variation_data' == [] and == ['ssm'] for cases with mutations
        for row in df:
            if row.case_id in empty_cases:
                assert row.available_variation_data == []
            else:
                assert row.available_variation_data == ['ssm']

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
