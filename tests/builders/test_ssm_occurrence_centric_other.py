import pytest

from exports.mappers import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('ssm_occurrence_centric_df')
class TestSSMOccurrenceCentricOther:

    @pytest.mark.parametrize('path', [
                             'case',
                             'case.available_variation_data',
                             'case.observation',
                             'ssm',
                             'ssm.consequence',
                             'ssm.consequence.transcript',
                             'ssm.consequence.transcript.gene',
                             'ssm.consequence.transcript.gene.symbol',
                             'ssm.consequence.transcript.gene.biotype',
                             'ssm.consequence.transcript.annotation',
                             ])
    def test_ssm_occurrence_centric_path_exists(self, ssm_occurrence_centric_df, path):
        """
        Chosen paths that have to be present to merge branch
        """
        ssm_occurrence_centric_df.select(path)

    @pytest.mark.do_not_collect
    @pytest.mark.parametrize('path',
                             ModelMapper('ssm_occurrence_centric').get_paths())
    @pytest.mark.skipif(conf.skip_in_depth_tests,
                        reason='we want to merge partial data fixes.'\
                        'This test is used for missing fields lookup.')
    def test_all_paths_ssm_occurrence(self, ssm_occurrence_centric_df, path):
        """
        Check for existence of all paths that are in mapping
        Can be skipped with skip_id_depth_tests switch
        """
        ssm_occurrence_centric_df.select(path)
