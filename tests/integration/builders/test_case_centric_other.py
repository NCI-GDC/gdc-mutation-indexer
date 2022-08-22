import pytest

from normalizer.mapper import ModelMapper
from tests.integration.utils.true_stats import TestDataStats
from tests.integration.config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'cnv_df', 'all_cases', 'all_maf_cases',
                         'case_centric_df', 'test_data')
class TestCaseCentricOther:
    """ Other case centric tests """

    def test_empty_cases(self, case_centric_df, maf_df, cnv_df,
                         test_data, all_cases, all_maf_cases):
        """
        Test "empty cases"

        Confirm that we index cases even if they have no maf or cnv data.
        This is necessary for the portal to visualize such cases.
        """
        cases_built = {c.case_id for c in case_centric_df.collect()}
        stats = TestDataStats.get_stats(maf_df, cnv_df, test_data,
                                        'case_centric')

        empty_cases = {
            c for c in all_cases
            if c not in all_maf_cases and c not in stats['cnv_cases']
        }

        # confirm that we have at least one empty case in our test data
        assert empty_cases

        # check that all cases were built (even empty ones)
        assert all_cases - cases_built == set()

    def test_available_variation_data(self, case_centric_df, maf_df, cnv_df,
                                      all_cases, all_maf_cases, test_data):
        """
        Test that available_variation_data is correctly populated:
            * ['cnv'] - for cnv-only cases
            * ['ssm'] - for cases in the maf header that do not have cnv data
            * ['cnv', 'ssm'] - for cases that have both maf and cnv data
            * [] - for cases that have no maf or cnv data
        """

        assert 'available_variation_data' in case_centric_df.columns

        gistic_cases = {r.case_id for r in cnv_df.collect()}
        maf_cases = all_maf_cases  # includes cases in maf header without ssms

        common_cases = gistic_cases & maf_cases
        cnv_cases = gistic_cases - maf_cases
        ssm_cases = maf_cases - gistic_cases
        empty_cases = all_cases - gistic_cases - maf_cases

        assert common_cases, 'there were no common cases found in test data'
        assert cnv_cases, 'there were no cnv cases found in test data'
        assert ssm_cases, 'there were no ssm cases found in test data'
        assert empty_cases, 'there were no empty cases found in test data'

        # Check that 'available_variation_data' is populated correctly
        for row in case_centric_df.collect():
            if row.case_id in common_cases:
                assert row.available_variation_data == ['cnv', 'ssm']
            elif row.case_id in cnv_cases:
                assert row.available_variation_data == ['cnv']
            elif row.case_id in ssm_cases:
                assert row.available_variation_data == ['ssm']
            elif row.case_id in empty_cases:
                assert row.available_variation_data == []

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

    @pytest.mark.do_not_collect
    @pytest.mark.skipif(conf.skip_in_depth_tests,
                        reason='we want to merge partial data fixes.'
                        'This test is used for missing fields lookup.')
    @pytest.mark.parametrize('path', ModelMapper('case_centric').get_paths())
    def test_all_paths_case(self, case_centric_df, path):
        """
        Check for existence of all paths that are in mapping
        Can be skipped with skip_id_depth_tests switch
        """
        case_centric_df.select(path)

