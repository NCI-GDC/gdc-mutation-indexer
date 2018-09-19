import pytest

from exports.mappers.model_mapper import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('ssm_centric_df')
class TestSSMCentricOther:

    @pytest.mark.parametrize('path', [
                             'occurrence',
                             'occurrence.occurrence_id',
                             'occurrence.case',
                             'occurrence.case.available_variation_data',
                             'consequence',
                             'consequence.consequence_id',
                             'consequence.transcript',
                             'consequence.transcript.gene',
                             'consequence.transcript.gene.symbol',
                             'consequence.transcript.gene.biotype',
                             'consequence.transcript.gene.gene_strand',
                             'consequence.transcript.annotation',
                             ])
    def test_ssm_centric_path_exists(self, ssm_centric_df, path):
        """
        Chosen paths that have to be present to merge branch
        """
        ssm_centric_df.select(path)

    @pytest.mark.do_not_collect
    @pytest.mark.parametrize('path', ModelMapper('ssm_centric').get_paths())
    @pytest.mark.skipif(conf.skip_in_depth_tests,
                        reason='we want to merge partial data fixes.'\
                        'This test is used for missing fields lookup.')
    def test_all_paths_ssm(self, ssm_centric_df, path):
        """
        Check for existence of all paths that are in mapping
        Can be skipped with skip_id_depth_tests switch
        """
        ssm_centric_df.select(path)
