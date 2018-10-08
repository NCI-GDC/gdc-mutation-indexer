import pytest

from exports.mappers.model_mapper import ModelMapper
from utils.true_stats import TestDataStats
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'gistic_df', 'all_maf_cases',
                         'case_centric_df', 'test_data')
class TestCaseCentricOther:
    """ Other case centric tests """

    def test_empty_cases(self, case_centric_df, maf_df, gistic_df,
                         test_data, all_maf_cases):
        """
        "empty cases" - cases that have been tested for ssms and there were no ssm found
        in other words, they are in the maf header as aliquots, but not in maf rows
        """
        cases_built = {c.case_id for c in case_centric_df.collect()}
        stats = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                        'case_centric')

        empty_cases = {c for c in all_maf_cases if c not in stats['ssm_cases']}

        # check that there was at least one empty case
        assert empty_cases

        # check that all maf cases were built (even empty ones)
        assert all_maf_cases - cases_built == set()

    def test_available_variation_data(self, case_centric_df, maf_df, gistic_df,
                                      all_maf_cases, test_data):
        """
        Test that available_variation_data is correctly populated:
            * ['cnv'] - for cnv-only genes
            * ['ssm'] - for ssm-only genes
            * ['cnv', 'ssm'] - for genes that have both ssms and cnvs

        """

        assert 'available_variation_data' in case_centric_df.columns

        gistic_cases = {r.case_id for r in gistic_df.collect()}
        maf_cases = all_maf_cases  # also includes "empty cases"

        common_cases = gistic_cases & maf_cases
        cnv_cases = gistic_cases - maf_cases
        ssm_cases = maf_cases - gistic_cases

        assert common_cases, 'there were no common cases found in test data'
        assert cnv_cases, 'there were no cnv cases found in test data'
        assert ssm_cases, 'there were no ssm cases found in test data'

        # Check that 'available_variation_data' is populated correctly
        for row in case_centric_df.collect():
            if row.case_id in common_cases:
                assert row.available_variation_data == ['cnv', 'ssm']
            elif row.case_id in cnv_cases:
                assert row.available_variation_data == ['cnv']
            elif row.case_id in ssm_cases:
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

