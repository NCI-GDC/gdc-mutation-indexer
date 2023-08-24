import logging

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Self

from exports import builders
from exports.builders import df_builders
from exports.configuration import adapter
from exports.constants import app

logging.basicConfig(format=app.LOG_FORMAT)


class SSMOccurrenceCentricBuilder(builders.BaseBuilder):
    """
    Builds ssm-occurrence-centric dataframe given case and maf dataframes::

        ssm_occurrence{}
              |____ ssm{}
              |        |____ consequence[]
              |                     |_____ transcript{}
              |                                   |_____ gene{}
              |                                   |_____ annotation{}
              |____ case{}
                       |____ observation[]
    """

    index_name = "ssm_occurrence_centric"
    id_field = "ssm_occurrence_id"

    def __init__(
        self,
        config: adapter.ObsoleteConfig,
        sqlContext: sql.SQLContext,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(
        self,
        maf_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        **kwargs: sql.DataFrame,
    ) -> Self:
        """
        Builds SSM Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "read":
            self.ssm_occurrence_centric = self.load_raw()
            if self.ssm_occurrence_centric is not None:
                return self

        case_obs_df = self.build_case_subtree(maf_df, case_df, primary_aliquot_df)

        ssm_cons = self.build_ssm_subtree(maf_df)

        self.log("Joining ssm with case")
        ssm_occurrence_centric = (
            ssm_cons.join(case_obs_df, on=["case_id", "ssm_id"], how="inner")
            .withColumn("ssm_occurrence_id", F.col("occurrence_id"))
            .drop("case_id")
            .drop("ssm_id")
            .drop("occurrence_id")
        )
        self.log_count(ssm_occurrence_centric)

        self.ssm_occurrence_centric = ssm_occurrence_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_ssm_subtree(self, maf_df):
        # Consequence
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df, self.index_name, join_gene=True,
        )

        # SSM
        ssm_df = df_builders.build_ssm_subtree(maf_df, cons_df, self.index_name).drop(
            "gene_id"
        )
        self.log_count(ssm_df)

        ssm_cons = ssm_df.select(
            "ssm_id",
            "case_id",
            F.struct(
                "consequence", *ssm_df.drop("consequence").drop("case_id").columns
            ).alias("ssm"),
        )
        self.log_count(ssm_cons)

        return ssm_cons

    def build_case_subtree(
        self,
        maf_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        self.log("Building case subtree")
        # Observation
        obs_df = self.observation_builder.build_for_ssm(
            maf_df, primary_aliquot_df, self.index_name,
        )

        self.log("Join observation with case")
        case_obs_df = case_df.join(obs_df, on=["case_id"], how="right").select(
            "case_id",
            "ssm_id",
            "occurrence_id",
            F.struct("observation", *case_df.columns).alias("case"),
        )
        self.log_count(case_obs_df)

        return case_obs_df
