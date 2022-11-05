import pytest

from tests.integration.utils.json_metrics import (
    GeneCentricStats,
    SSMCentricStats,
    SSMOccurrenceCentricStats,
)
from tests.integration.config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures(
    "maf_df", "cnv_df", "gene_centric_df", "es_client"
)
class TestGeneCentricData:
    @pytest.mark.parametrize(
        "stat", ["Nprojects", "Ncases", "Ngenes", "NUniqMut", "Nconseq"]
    )
    @pytest.mark.skipif(conf.indices_are_pruned, reason="n/a if pruned")
    def test_gene_centric_summary_stats(self, es_client, maf_stats, stat):
        docs = es_client.search(
            index=conf.indices["gene_centric"],
            body={"query": {"match_all": {}}},
            size=1000,
        )["hits"]["hits"]
        gene_stats = GeneCentricStats(docs)

        gene_stat = getattr(gene_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert gene_stat == maf_stat


@pytest.mark.usefixtures(
    "maf_df", "cnv_df", "ssm_centric_df", "es_client"
)
class TestSSMCentricData:
    @pytest.mark.parametrize(
        "stat", ["Nprojects", "Ncases", "Ngenes", "NUniqMut", "Nconseq"]
    )
    @pytest.mark.skipif(conf.indices_are_pruned, reason="n/a if pruned")
    def test_ssm_centric_summary_stats(self, maf_stats, stat, es_client):
        docs = es_client.search(
            index=conf.indices["ssm_centric"],
            body={"query": {"match_all": {}}},
            size=1000,
        )["hits"]["hits"]
        ssm_stats = SSMCentricStats(docs)

        ssm_stat = getattr(ssm_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_stat == maf_stat


@pytest.mark.usefixtures(
    "maf_df", "cnv_df", "ssm_occurrence_centric_df", "es_client"
)
class TestSSMOccurrenceCentricData:
    @pytest.mark.parametrize(
        "stat", ["Nprojects", "Ncases", "Ngenes", "NUniqMut", "Nconseq"]
    )
    @pytest.mark.skipif(conf.indices_are_pruned, reason="n/a if pruned")
    def test_ssm_occurrence_centric_summary_stats(self, maf_stats, stat, es_client):
        docs = es_client.search(
            index=conf.indices["ssm_occurrence_centric"],
            body={"query": {"match_all": {}}},
            size=1000,
        )["hits"]["hits"]
        ssm_occurrence_stats = SSMOccurrenceCentricStats(docs)
        ssm_occ_stat = getattr(ssm_occurrence_stats, stat)
        maf_stat = getattr(maf_stats, stat)
        assert ssm_occ_stat == maf_stat
