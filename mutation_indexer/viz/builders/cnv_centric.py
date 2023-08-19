import logging

from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_set, struct
from typing_extensions import Self

from mutation_indexer.builders import base_builder
from mutation_indexer.configuration.old_adapter import LOG_FORMAT, BaseConfig
from mutation_indexer.viz.builders import consequence, df_builders, observation

logging.basicConfig(format=LOG_FORMAT)


class CNVCentricBuilder(base_builder.BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case, gene, and maf dataframes:

     cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                      |_____ case{}
                                |____ observation[]

    """

    index_name = "cnv_centric"
    id_field = "cnv_id"

    def __init__(
        self,
        config: BaseConfig,
        sqlContext: SQLContext,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(
        self, ascat_df: sql.DataFrame, case_df: sql.DataFrame, **kwargs: sql.DataFrame
    ) -> Self:
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "load":
            self.cnv_centric = self.load_raw()
            if self.cnv_centric is not None:
                return self

        self.log("Select CNV data from ASCAT")
        cnv_df = df_builders.get_cnv_df(ascat_df, self.index_name)

        self.log("Build Consequence")
        cons_df = self.consequence_builder.build_for_cnv(
            ascat_df,
            self.index_name,
        )

        self.log("Build Occurrence")
        occurrence_df = self.build_occurrence_df(ascat_df, case_df)

        self.log("Final join CNV + Consequence + Occurrence")
        cnv_cons_df = cnv_df.join(cons_df, on="cnv_id", how="left")
        cnv_centric_df = cnv_cons_df.join(occurrence_df, on="cnv_id", how="left")

        # truncate outliers
        threshold = self.config.percentile_threshold["occurrences_per_cnv"]
        cnv_centric_df = self.truncate_df_at_percentile(
            cnv_centric_df, "occurrence", threshold
        )

        # save final df as property
        self.cnv_centric = cnv_centric_df

        self.log_count(self.cnv_centric)
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_occurrence_df(self, ascat_df, case_df):
        """
        Assumes you've already added 'case_id'

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        assert "case_id" in ascat_df.columns

        # 1. Observation
        self.logger.info("Aggregating Observation from ASCAT")
        obs_df = self.observation_builder.build_for_cnv(
            ascat_df,
            self.index_name,
        )

        # 2. Join Case to Observation and create structs
        self.logger.info("Joining Cases with Observation, [right, case_id]")
        occurrence_df = (
            case_df.join(obs_df, on=["case_id"], how="left")
            .select(
                "cnv_id",
                struct(
                    "occurrence_id",
                    struct("observation", *case_df.columns).alias("case"),
                ).alias("occurrence"),
            )
            .groupby("cnv_id")
            .agg(collect_set("occurrence").alias("occurrence"))
        )

        return occurrence_df
