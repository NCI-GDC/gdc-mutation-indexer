import pprint

import deepdiff
import pytest
from pyspark import sql
from pyspark.sql import functions as F

from exports.builders import utils
from tests.integration.builders import base_joins_test


@pytest.mark.usefixtures("maf_df", "ssm_centric_df", "ssm_transcript_df")
class TestSSMCentricJoins(base_joins_test.BaseJoinsTest):
    """
    ssm{}
      |____ consequence[]
      |           |_____ transcript{}
      |                        |_____ gene{}
      |                        |_____ annotation{}
      |____ occurrence[]
                  |_____ case{}
                           |____ observation[]
    """

    def test_consequences_per_ssm(
        self, ssm_centric_df: sql.DataFrame, ssm_transcript_df: sql.DataFrame
    ) -> None:

        # Consequences per SSM built:
        df = self.unpack_df_list(
            ssm_centric_df, "ssm_id", "consequence", "consequence_id"
        )
        cps = self.get_relationship_map(df, "ssm_id", "consequence_id")

        # Consequences per SSM expected:
        # Consequence ~ UUID[ssm_id, transcript_id]
        df = ssm_transcript_df.withColumn(
            "consequence_id",
            utils.uuid5_col(
                F.lit("ssm_consequence"), F.col("ssm_id"), F.col("transcript_id")
            ),
        )

        true_cps = self.get_relationship_map(df, "ssm_id", "consequence_id")

        pprint.pprint(deepdiff.DeepDiff(cps, true_cps))

        assert cps == true_cps


@pytest.mark.usefixtures("ssm_centric_df")
class TestSSMCentricOther:
    @pytest.mark.parametrize(
        "path",
        [
            "occurrence",
            "occurrence.occurrence_id",
            "occurrence.case",
            "occurrence.case.available_variation_data",
            "consequence",
            "consequence.consequence_id",
            "consequence.transcript",
            "consequence.transcript.gene",
            "consequence.transcript.gene.symbol",
            "consequence.transcript.gene.biotype",
            "consequence.transcript.gene.gene_strand",
            "consequence.transcript.annotation",
        ],
    )
    def test_ssm_centric_path_exists(
        self, ssm_centric_df: sql.DataFrame, path: str
    ) -> None:
        """
        Chosen paths that have to be present to merge branch
        """
        ssm_centric_df.select(path)
