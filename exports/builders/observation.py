import logging
from typing import Optional

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

import config
from exports.builders import utils

logging.basicConfig(format=config.LOG_FORMAT)


class ObservationBuilder:
    """
    Builds observation dataframe from the maf dataframe
    """

    def build_for_ssm(
        self,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        index_name: str,
        selector: Optional[str] = None,
    ) -> sql.DataFrame:
        """
        Builds an observation from a maf.
        Each line of a maf is roughly an observation, though it could be better
        said that a unique observation is identified by a unqiue pairing of
        tumor and normal sample uuids and an ssm uuid.
        """
        # Select all of the nested fields
        primary_aliquot_df = primary_aliquot_df.where(F.col("entity") == F.lit("case"))
        flat_obs_df = maf_df.select(
            "ssm_id",
            "case_id",
            "occurrence_id",
            *utils.select_nested(
                index_name, "observation", selector=selector, ignore=["observation_id"]
            )
        ).join(primary_aliquot_df, ["case_id"], how="left")

        # This will be used to explode variant_caller column
        # TODO: TECH DEBT - THIS CAN BE EASILY CONVERTED TO NATIVE SPARK
        variant_caller = F.udf(
            utils.transform_variant_caller,
            types.ArrayType(types.StringType()),
        )

        flat_obs_df = (
            flat_obs_df.
            # Explode variant_caller into multiple observations
            withColumn(
                "variant_caller", F.explode(variant_caller(F.col("variant_caller")))
            ).
            # Add observation_id
            withColumn(
                "observation_id",
                utils.uuid5_col(
                    F.lit("ssm_observation"),
                    F.col("occurrence_id"),
                    F.col("tumor_sample_uuid"),
                    F.col("matched_norm_sample_uuid"),
                    F.col("variant_caller"),
                    F.lit("masked"),
                ),
            )
        )

        obs_df = (
            flat_obs_df.select(
                "ssm_id",
                "case_id",
                "occurrence_id",
                F.struct(
                    *utils.struct_select(index_name, "observation", selector=selector)
                ).alias("observation"),
            )
            .groupby("ssm_id", "case_id", "occurrence_id")
            .agg(F.collect_list("observation").alias("observation"))
        )

        return obs_df

    def build_for_cnv(
        self, ascat_df: sql.DataFrame, index: str, selector: Optional[str] = None
    ) -> sql.DataFrame:
        """
        observation[]
        |____ observation{}
                |____ observation_id
                |____ variant_status
                |____ variant_calling {}
                        |____ variant_caller

        """

        # add other observation fields
        obs_df = ascat_df.withColumn(
            "variant_calling", F.struct("variant_caller").alias("variant_calling")
        )

        # observation structure
        obs_df = (
            obs_df.select(
                "cnv_id",
                "case_id",
                "occurrence_id",
                F.struct(
                    *utils.struct_select(index, "observation", selector=selector)
                ).alias("observation"),
            )
            .groupby("cnv_id", "case_id", "occurrence_id")
            .agg(F.collect_set("observation").alias("observation"))
        )

        return obs_df
