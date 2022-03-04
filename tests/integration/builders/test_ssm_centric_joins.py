import pytest
from pyspark.sql import functions as F

from mutation_indexer.builders import utils
from tests.integration import config
from tests.integration.builders import base_joins_test

conf = config.TestConfig()


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

    def test_consequences_per_ssm(self, maf_df, ssm_centric_df, ssm_transcript_df):

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
        assert cps == true_cps

    def test_occurrences_per_ssm(self, maf_df, ssm_centric_df):
        # Occurrences per SSM built:
        df = self.unpack_df_list(
            ssm_centric_df, "ssm_id", "occurrence", "occurrence_id"
        )
        ops = self.get_relationship_map(df, "ssm_id", "occurrence_id")

        # Occurrences per SSM expected:
        df = maf_df.select("ssm_id", "occurrence_id").distinct()
        true_ops = self.get_relationship_map(df, "ssm_id", "occurrence_id")

        assert ops == true_ops
