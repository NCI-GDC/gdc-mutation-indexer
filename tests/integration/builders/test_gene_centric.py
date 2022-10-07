import json

import pytest
from pyspark import sql
from pyspark.sql import functions as F

from exports import builders, configuration, es_utils
from exports.constants import build
from tests.integration.builders import base_joins_test


@pytest.mark.usefixtures("maf_df", "cnv_df", "gene_centric_df", "gene_ssm_subtree")
class TestGeneCentricJoins(base_joins_test.BaseJoinsTest):
    """
    Test case_centric index joins

        gene{}
             |___ case[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]

    """

    def test_cases_per_gene(
        self,
        maf_df: sql.DataFrame,
        cnv_df: sql.DataFrame,
        gene_centric_df: sql.DataFrame,
    ) -> None:
        # Cases per gene built:
        df = self.unpack_df_list(gene_centric_df, "gene_id", "case", "case_id")
        cpg = self.get_relationship_map(df, "gene_id", "case_id")

        # Cases per gene expected:
        df = (
            maf_df.select("case_id", "gene_id").union(
                cnv_df.select("case_id", "gene_id")
            )
        ).distinct()
        true_cpg = self.get_relationship_map(df, "gene_id", "case_id")

        assert cpg == true_cpg

    def test_ssm_per_case(
        self, maf_df: sql.DataFrame, gene_centric_df: sql.DataFrame
    ) -> None:
        # SSMs per case built:
        df = self.unpack_df_list(gene_centric_df, "gene_id", "case", ("case_id", "ssm"))
        df = self.unpack_df_list(df, ("gene_id", "case_id"), "ssm", "ssm_id")

        es_spc = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_spc.setdefault(row["gene_id"], {})
            es_spc[row["gene_id"]].setdefault(row["case_id"], set())
            es_spc[row["gene_id"]][row["case_id"]].update({row["ssm_id"]})

        # SSMs per case expected:
        spc = {}
        df = maf_df.select("gene_id", "case_id", "ssm_id")
        for row in df.toJSON().collect():
            row = json.loads(row)
            spc.setdefault(row["gene_id"], {})
            spc[row["gene_id"]].setdefault(row["case_id"], set())
            spc[row["gene_id"]][row["case_id"]].update({row["ssm_id"]})

        assert es_spc == spc

    @pytest.mark.gene_centric_ssm_subtree
    @pytest.mark.usefixtures("gene_centric_df")
    def test_ssm_subtree(
        self,
        observation_builder: builders.ObservationBuilder,
        consequence_builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        gene_ssm_subtree: sql.DataFrame,
    ) -> None:
        def get_stats(dataframe):
            """
            Extracts ssm, consequence, transcript relationships from a flat dataframe
            """
            res = {}
            for row in dataframe.toJSON().collect():
                row = json.loads(row)
                sid = row["ssm_id"]
                oid = row["observation_id"]
                cid = row["consequence_id"]

                res.setdefault(sid, {"consequences": set(), "observations": set()})
                res[sid]["consequences"].update([cid])
                res[sid]["observations"].update([oid])

            return res

        # ssm_subtree stats expected:
        cons_df = consequence_builder.build_for_ssm(maf_df, "gene_centric")
        obs_df = observation_builder.build_for_ssm(
            maf_df, primary_aliquot_df, "gene_centric", selector="ssm"
        )

        df = cons_df.join(obs_df, on=["ssm_id"], how="left")

        df = self.unpack_df_list(
            df, ("ssm_id", "observation"), "consequence", "consequence_id"
        )
        df = self.unpack_df_list(
            df, ("ssm_id", "consequence_id"), "observation", "observation_id"
        )
        true_stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(
            gene_ssm_subtree, (), "ssm", ("ssm_id", "consequence", "observation")
        )
        df = self.unpack_df_list(
            df, ("ssm_id", "observation"), "consequence", "consequence_id"
        )
        df = self.unpack_df_list(
            df, ("ssm_id", "consequence_id"), "observation", "observation_id"
        )
        stats = get_stats(df)

        assert stats == true_stats


def test_gene_centric_paths_exist(gene_centric_df: sql.DataFrame) -> None:
    """
    Chosen paths that have to be present to merge branch
    """
    columns = frozenset(gene_centric_df.columns)

    assert columns.issuperset(("gene_id", "transcripts", "case"))

    case_df = gene_centric_df.select(F.explode("case").alias("case")).select("case.*")
    case_columns = frozenset(case_df.columns)
    transcript_df = gene_centric_df.select(
        F.explode("transcripts").alias("transcript")
    ).select("transcript.*")
    transcript_columns = frozenset(transcript_df.columns)

    assert "case_id" in case_columns
    assert transcript_columns.issuperset(("is_canonical", "exons", "domains"))


@pytest.mark.usefixtures("gene_centric_df")
def test_gene_centric_count(
    default_config: configuration.Configuration,
    es_client: sql.DataFrame,
    maf_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
) -> None:
    expected_count = (
        maf_df.select("gene_id").union(cnv_df.select("gene_id")).distinct().count()
    )
    built_count = es_utils.get_es_doc_count(
        es_client,
        default_config.elasticsearch.write.indices[build.IndexType.GENE_CENTRIC],
    )

    assert built_count == expected_count
