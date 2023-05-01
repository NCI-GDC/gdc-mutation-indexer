from pyspark import sql
from pyspark.sql import functions as F

from exports.builders import utils
from tests.integration.utils import join_utils


def test_consequences_per_ssm(
    ssm_centric_df: sql.DataFrame, ssm_transcript_df: sql.DataFrame
) -> None:
    # Consequences per SSM built:
    df = join_utils.unpack_df_list(
        ssm_centric_df, "ssm_id", "consequence", "consequence_id"
    )
    cps = join_utils.get_relationship_map(df, ("ssm_id", "consequence_id"))

    # Consequences per SSM expected:
    # Consequence ~ UUID[ssm_id, transcript_id]
    df = ssm_transcript_df.withColumn(
        "consequence_id",
        utils.uuid5_col(
            F.lit("ssm_consequence"), F.col("ssm_id"), F.col("transcript_id")
        ),
    )

    true_cps = join_utils.get_relationship_map(df, ("ssm_id", "consequence_id"))

    assert cps == true_cps
