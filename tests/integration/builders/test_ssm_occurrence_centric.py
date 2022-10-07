import json

import pytest
from pyspark import sql
from pyspark.sql import functions as F

from exports import builders
from exports.builders import utils
from tests.integration.builders import base_joins_test


@pytest.mark.usefixtures("ssm_occurrence_centric_df")
class TestSSMOccurrenceCentricOther:
    @pytest.mark.parametrize(
        "path",
        [
            "case",
            "case.available_variation_data",
            "case.observation",
            "ssm",
            "ssm.consequence",
            "ssm.consequence.transcript",
            "ssm.consequence.transcript.gene",
            "ssm.consequence.transcript.gene.symbol",
            "ssm.consequence.transcript.gene.biotype",
            "ssm.consequence.transcript.annotation",
        ],
    )
    def test_ssm_occurrence_centric_path_exists(
        self, ssm_occurrence_centric_df: sql.DataFrame, path: str
    ) -> None:
        """
        Chosen paths that have to be present to merge branch
        """
        ssm_occurrence_centric_df.select(path)


@pytest.mark.usefixtures(
    "maf_df",
    "ssm_transcript_df",
    "ssm_occurrence_centric_df",
    "ssm_occurrence_ssm_subtree",
)
class TestSSMOccurrenceCentricJoins(base_joins_test.BaseJoinsTest):
    """
    ssm_occurrence{}
          |____ ssm{}
          |        |____ consequence[]
          |                     |_____ transcript{}
          |                                   |_____ gene{}
          |                                   |_____ annotation{}
          |____ case{}
                   |____ observation[]
    """

    def test_consequences_per_ssm_occurrence(
        self, ssm_transcript_df: sql.DataFrame, ssm_occurrence_centric_df: sql.DataFrame
    ) -> None:

        # Consequences per SSM Occurrence built:
        df = ssm_occurrence_centric_df.select("ssm_occurrence_id", "ssm.ssm_id")
        ssm_occ_to_ssm = self.get_relationship_map(df, "ssm_occurrence_id", "ssm_id")

        # SSM to SSM Occurrence expected:
        df = (
            ssm_transcript_df.withColumn(
                "consequence_id",
                utils.uuid5_col(
                    F.lit("ssm_consequence"), F.col("ssm_id"), F.col("transcript_id")
                ),
            )
            .withColumn("ssm_occurrence_id", F.col("occurrence_id"))
            .select(
                "ssm_occurrence_id",
                "ssm_id",
                "consequence_id",
                "transcript_id",
                "gene_id",
            )
        )

        true_ssm_occ_to_ssm = self.get_relationship_map(
            df, "ssm_occurrence_id", "ssm_id"
        )

        assert ssm_occ_to_ssm == true_ssm_occ_to_ssm

    def test_observations_per_ssm_occurrence(
        self, maf_df: sql.DataFrame, ssm_occurrence_centric_df: sql.DataFrame
    ) -> None:

        # The observation ID is calculated when the observation dataframe is built,
        # and is not included in the MAF dataframe, so we can't compute the "true"
        # occurrence ID -> observation ID mapping based on the MAF dataframe alone.
        # Use the tumor sample barcode as an approximation of observation ID.

        # Tumor samples and Cases per SSM Occurrence built:
        df = self.unpack_df_list(
            ssm_occurrence_centric_df,
            ["ssm_occurrence_id", "case.case_id"],
            "case.observation",
            "sample.tumor_sample_barcode",
        )

        spo = self.get_relationship_map(df, "ssm_occurrence_id", "tumor_sample_barcode")
        cpo = self.get_relationship_map(df, "ssm_occurrence_id", "case_id")

        # Observations and Cases per SSM Occurrence expected:
        df = maf_df.withColumn("ssm_occurrence_id", F.col("occurrence_id")).select(
            "ssm_occurrence_id", "case_id", "tumor_sample_barcode"
        )

        true_spo = self.get_relationship_map(
            df, "ssm_occurrence_id", "tumor_sample_barcode"
        )
        true_cpo = self.get_relationship_map(df, "ssm_occurrence_id", "case_id")

        assert spo == true_spo
        assert cpo == true_cpo

    @pytest.mark.ssm_occurrence_centric_ssm_subtree
    def test_ssm_subtree(
        self, maf_df: sql.DataFrame, ssm_occurrence_ssm_subtree: sql.DataFrame
    ) -> None:
        def get_stats(dataframe: sql.DataFrame) -> dict:
            """
            Extracts ssm, consequence, transcript, gene relationships
            from a flat dataframe
            """
            res = {}
            for row in dataframe.collect():
                row = row.asDict(recursive=True)
                sid = row["ssm_id"]
                cid = row["consequence_id"]
                tid = row["transcript_id"]
                gid = row["gene_id"]

                res.setdefault(sid, {})
                res[sid].setdefault(cid, {"transcripts": set(), "genes": set()})
                res[sid][cid]["transcripts"].update([tid])
                res[sid][cid]["genes"].update([gid])

            return res

        fields_to_unpack = [
            "consequence_id",
            "transcript.transcript_id",
            "transcript.gene.gene_id",
        ]

        # ssm_subtree stats expected:
        cons_df = builders.ConsequenceBuilder(None, None).build_for_ssm(
            maf_df, "ssm_occurrence_centric", join_gene=True
        )
        df = self.unpack_df_list(cons_df, "ssm_id", "consequence", fields_to_unpack)
        true_stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(
            ssm_occurrence_ssm_subtree, "ssm_id", "ssm.consequence", fields_to_unpack
        )
        stats = get_stats(df)

        assert stats == true_stats
