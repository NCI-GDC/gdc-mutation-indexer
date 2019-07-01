import pytest

from exports.mappers import ModelMapper
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('gene_centric_df')
class TestGeneCentricOther:
    """
    Test intermediate result from the case centric builder
    """
    @pytest.mark.parametrize('path', [
                             'gene_id',
                             'transcripts',
                             'transcripts.is_canonical',
                             'transcripts.exons',
                             'transcripts.domains',
                             'case',
                             'case.case_id',
                             ])
    def test_gene_centric_path_exists(self, gene_centric_df, path):
        """
        Chosen paths that have to be present to merge branch
        """
        gene_centric_df.select(path)

    @pytest.mark.do_not_collect
    @pytest.mark.parametrize('path', ModelMapper('gene_centric').get_paths())
    @pytest.mark.skipif(conf.skip_in_depth_tests,
                        reason='we want to merge partial data fixes.'
                        'This test is used for missing fields lookup.')
    def test_all_paths_gene(self, gene_centric_df, path):
        """
        Check for existence of all paths that are in mapping
        Can be skipped with skip_id_depth_tests switch
        """
        gene_centric_df.select(path)
