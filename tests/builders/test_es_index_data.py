import pytest

from utils.true_stats import TestDataStats
from utils.json_metrics import (
    CaseCentricStats,
    GeneCentricStats,
    SSMCentricStats,
    SSMOccurrenceCentricStats
)
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'test_data', 'case_centric_index')
class TestCaseCentricData:

    def test_case_centric_count(self, maf_df, test_data, case_centric_index):
        expected_count = TestDataStats.get_stats(maf_df, test_data,
                                                 'case_centric')['count']
        built_count = case_centric_index.count(
            index=conf.indices['case_centric'],
            doc_type='case_centric',
            body={"query": {"match_all": {}}}
        )['count']
        assert built_count == expected_count

    @pytest.mark.parametrize('stat', ['Nprojects',
                                      'Ncases',
                                      'Ngenes',
                                      'NUniqMut',
                                      'Nconseq'])
    def test_case_centric_summary_stats(self, case_centric_index, maf_stats, stat):
        docs = case_centric_index.search(
            index=conf.indices['case_centric'],
            doc_type='case_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        case_stats = CaseCentricStats(docs)

        case_stat = getattr(case_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        if conf.indices_are_pruned:
            if stat in ['Ngenes', 'NUniqMut', 'Nconseq']:
                return
        assert case_stat == maf_stat


@pytest.mark.usefixtures('maf_df', 'test_data', 'gene_centric_index')
class TestGeneCentricData:

    def test_gene_centric_count(self, maf_df, test_data, gene_centric_index):
        expected_count = TestDataStats.get_stats(maf_df, test_data,
                                                 'gene_centric')['count']
        built_count = gene_centric_index.count(
            index=conf.indices['gene_centric'],
            doc_type='gene_centric',
            body={"query": {"match_all": {}}}
        )['count']
        assert built_count == expected_count

    @pytest.mark.parametrize('stat', ['Nprojects',
                                      'Ncases',
                                      'Ngenes',
                                      'NUniqMut',
                                      'Nconseq'])
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_gene_centric_summary_stats(self, gene_centric_index, maf_stats, stat):
        docs = gene_centric_index.search(
            index=conf.indices['gene_centric'],
            doc_type='gene_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        gene_stats = GeneCentricStats(docs)

        gene_stat = getattr(gene_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert gene_stat == maf_stat


@pytest.mark.usefixtures('ssm_centric_index')
class TestSSMCentricData:

    def test_ssm_centric_count(self, maf_df, test_data, ssm_centric_index):
        expected_count = TestDataStats.get_stats(maf_df, test_data,
                                                 'ssm_centric')['count']
        built_count = ssm_centric_index.count(
            index=conf.indices['ssm_centric'],
            doc_type='ssm_centric',
            body={"query": {"match_all": {}}}
        )['count']
        assert built_count == expected_count

    @pytest.mark.parametrize('stat', ['Nprojects',
                                      'Ncases',
                                      'Ngenes',
                                      'NUniqMut',
                                      'Nconseq'])
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_ssm_centric_summary_stats(self, maf_stats, stat,
                                       ssm_centric_index):
        docs = ssm_centric_index.search(
            index=conf.indices['ssm_centric'],
            doc_type='ssm_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        ssm_stats = SSMCentricStats(docs)

        ssm_stat = getattr(ssm_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_stat == maf_stat


@pytest.mark.usefixtures('ssm_occurrence_centric_index')
class TestSSMOccurrenceCentricData:

    def test_ssm_occurrence_centric_count(self, maf_df, test_data,
                                          ssm_occurrence_centric_index):
        expected_count = TestDataStats.get_stats(maf_df, test_data,
                                                 'ssm_occurrence_centric')['count']
        built_count = ssm_occurrence_centric_index.count(
            index=conf.indices['ssm_occurrence_centric'],
            doc_type='ssm_occurrence_centric',
            body={"query": {"match_all": {}}}
        )['count']
        assert built_count == expected_count

    @pytest.mark.parametrize('stat', ['Nprojects',
                                      'Ncases',
                                      'Ngenes',
                                      'NUniqMut',
                                      'Nconseq'])
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_ssm_occurrence_centric_summary_stats(self, maf_stats, stat,
                                                  ssm_occurrence_centric_index):
        docs = ssm_occurrence_centric_index.search(
            index=conf.indices['ssm_occurrence_centric'],
            doc_type='ssm_occurrence_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        ssm_occurrence_stats = SSMOccurrenceCentricStats(docs)
        ssm_occ_stat = getattr(ssm_occurrence_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_occ_stat == maf_stat
