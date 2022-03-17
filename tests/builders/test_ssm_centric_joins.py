import pprint

import deepdiff
import pytest
from base_joins_test import BaseJoinsTest
from pyspark.sql import functions as F

from exports.builders.utils import uuid5_col
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures("maf_df", "ssm_centric_df", "ssm_transcript_df")
class TestSSMCentricJoins(BaseJoinsTest):
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
            uuid5_col(F.lit("ssm_consequence"), F.col("ssm_id"), F.col("transcript_id")),
        )

        true_cps = self.get_relationship_map(df, "ssm_id", "consequence_id")

        pprint.pprint(deepdiff.DeepDiff(cps, true_cps))

        assert cps == true_cps
