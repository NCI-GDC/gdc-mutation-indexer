import pytest

from exports.es_utils import get_es_doc_count
from utils.true_stats import TestDataStats
from utils.json_metrics import (
    CaseCentricStats,
    GeneCentricStats,
    SSMCentricStats,
    SSMOccurrenceCentricStats
)
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('gistic_df', 'cnv_centric_df', 'test_data', 'es_client')
class TestCNVCentricData:

    def test_cnv_centric_count(self, maf_df, gistic_df, test_data, es_client):
        doc_type = 'cnv_centric'
        expected_count = TestDataStats.get_stats(
            maf_df, gistic_df, test_data, doc_type)['count']
        built_count = get_es_doc_count(es_client, conf.indices[doc_type], doc_type)
        assert built_count == expected_count


@pytest.mark.usefixtures('gistic_df', 'test_data', 'es_client',
                         'cnv_occurrence_centric_df')
class TestCNVOccurrenceCentricData:

    def test_cnv_occurrence_centric_count(self, maf_df, gistic_df, test_data, es_client):
        doc_type = 'cnv_occurrence_centric'
        expected_count = TestDataStats.get_stats(
            maf_df, gistic_df, test_data, doc_type)['count']
        built_count = get_es_doc_count(es_client, conf.indices[doc_type], doc_type)
        assert built_count == expected_count


@pytest.mark.usefixtures('all_maf_cases',
                         'maf_df',
                         'gistic_df',
                         'case_centric_df',
                         'test_data',
                         'es_client')
class TestCaseCentricData:

    def test_case_centric_count(self, all_maf_cases,
                                maf_df, gistic_df, test_data, es_client):

        # Count of cases in maf + gistic dataframes
        stats = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                        'case_centric')

        # Take empty ssm cases into account
        empty_cases = {c for c in all_maf_cases if c not in stats['ssm_cases']}
        expected_count = stats['count'] + len(empty_cases)

        built_count = es_client.count(
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
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_case_centric_summary_stats(self, es_client,
                                        maf_stats, stat, all_maf_cases):
        docs = es_client.search(
            index=conf.indices['case_centric'],
            doc_type='case_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        case_stats = CaseCentricStats(docs)

        case_stat = getattr(case_stats, stat)
        maf_stat = getattr(maf_stats, stat)

        # Take empty cases into account
        if stat == 'Ncases':
            maf_stat = len(all_maf_cases)

        if conf.indices_are_pruned:
            if stat in ['Ngenes', 'NUniqMut', 'Nconseq']:
                return
        assert case_stat == maf_stat


@pytest.mark.usefixtures('maf_df', 'gistic_df', 'test_data',
                         'gene_centric_df', 'es_client')
class TestGeneCentricData:

    def test_gene_centric_count(self, maf_df, gistic_df, test_data, es_client):
        expected_count = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                                 'gene_centric')['count']
        built_count = es_client.count(
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
    def test_gene_centric_summary_stats(self, es_client, maf_stats, stat):
        docs = es_client.search(
            index=conf.indices['gene_centric'],
            doc_type='gene_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        gene_stats = GeneCentricStats(docs)

        gene_stat = getattr(gene_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert gene_stat == maf_stat


@pytest.mark.usefixtures('maf_df', 'gistic_df', 'test_data', 'ssm_centric_df', 'es_client')
class TestSSMCentricData:

    def test_ssm_centric_count(self, maf_df, gistic_df, test_data, es_client):
        expected_count = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                                 'ssm_centric')['count']
        built_count = es_client.count(
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
                                       es_client):
        docs = es_client.search(
            index=conf.indices['ssm_centric'],
            doc_type='ssm_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        ssm_stats = SSMCentricStats(docs)

        ssm_stat = getattr(ssm_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_stat == maf_stat


@pytest.mark.usefixtures('maf_df', 'gistic_df', 'test_data',
                         'ssm_occurrence_centric_df', 'es_client')
class TestSSMOccurrenceCentricData:

    def test_ssm_occurrence_centric_count(self, maf_df, gistic_df, test_data, es_client):
        expected_count = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                                 'ssm_occurrence_centric')['count']
        built_count = es_client.count(
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
    def test_ssm_occurrence_centric_summary_stats(self, maf_stats, stat, es_client):
        docs = es_client.search(
            index=conf.indices['ssm_occurrence_centric'],
            doc_type='ssm_occurrence_centric',
            body={"query": {"match_all": {}}},
            size=1000
        )['hits']['hits']
        ssm_occurrence_stats = SSMOccurrenceCentricStats(docs)
        ssm_occ_stat = getattr(ssm_occurrence_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_occ_stat == maf_stat
