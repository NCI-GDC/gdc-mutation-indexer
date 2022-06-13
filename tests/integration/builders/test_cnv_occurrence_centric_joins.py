import json

import pytest
from pyspark.sql import functions as F

from mutation_indexer.viz import builders
from tests.integration import config
from tests.integration.builders import base_joins_test

conf = config.TestConfig()


@pytest.mark.usefixtures("gistic_df", "cnv_occurrence_centric_df")
class TestCNVOccurrenceCentricJoins(base_joins_test.BaseJoinsTest):
    """
    cnv_occurrence{}
        |
        |____ case{}
        |       |____ observation[]
        |
        |____ cnv{}
                |____ consequence[]
                            |_____ gene{}

    """

    def test_consequences_per_cnv_occurrence(
        self, gistic_df, cnv_occurrence_centric_df
    ):
        # Consequences per CNV Occurrence built:
        df = cnv_occurrence_centric_df.select("cnv_occurrence_id", "cnv.cnv_id")
        cnv_occ_to_cnv = self.get_relationship_map(df, "cnv_occurrence_id", "cnv_id")

        # CNV to CNV Occurrence expected:
        df = gistic_df.withColumnRenamed("occurrence_id", "cnv_occurrence_id")
        true_cnv_occ_to_cnv = self.get_relationship_map(
            df, "cnv_occurrence_id", "cnv_id"
        )

        assert cnv_occ_to_cnv == true_cnv_occ_to_cnv

    def test_observations_per_cnv_occurrence(
        self, gistic_df, cnv_occurrence_centric_df
    ):
        # Observations and Cases per CNV Occurrence built:
        df = self.unpack_df_list(
            cnv_occurrence_centric_df,
            ["cnv_occurrence_id", "case.case_id"],
            "case.observation",
            "observation_id",
        )

        opo = self.get_relationship_map(df, "cnv_occurrence_id", "observation_id")
        cpo = self.get_relationship_map(df, "cnv_occurrence_id", "case_id")

        # Observations and Cases per CNV Occurrence expected:
        df = gistic_df.withColumn("cnv_occurrence_id", F.col("occurrence_id")).select(
            "cnv_occurrence_id", "observation_id", "case_id"
        )
        true_opo = self.get_relationship_map(df, "cnv_occurrence_id", "observation_id")
        true_cpo = self.get_relationship_map(df, "cnv_occurrence_id", "case_id")

        assert opo == true_opo
        assert cpo == true_cpo

    @pytest.mark.cnv_occurrence_centric_cnv_subtree
    def test_cnv_subtree(self, sqlContext, gistic_df, cnv_occurrence_centric_df):
        def get_stats(dataframe):
            """
            Extracts cnv, consequence, transcript, gene relationships
            from a flat dataframe
            """
            res = {}
            for row in dataframe.toJSON().collect():
                row = json.loads(row)
                sid = row["cnv_id"]
                cid = row["consequence_id"]
                gid = row["gene_id"]

                res.setdefault(sid, {})
                res[sid].setdefault(cid, set())
                res[sid][cid].update([gid])

            return res

        fields_to_unpack = ["consequence_id", "gene.gene_id"]

        # ssm_subtree stats expected:
        cons_df = builders.ConsequenceBuilder(conf, sqlContext).build_for_cnv(
            gistic_df, "cnv_occurrence_centric"
        )
        df = self.unpack_df_list(cons_df, "cnv_id", "consequence", fields_to_unpack)
        true_stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(
            cnv_occurrence_centric_df, "cnv.cnv_id", "cnv.consequence", fields_to_unpack
        )
        stats = get_stats(df)

        assert stats == true_stats
