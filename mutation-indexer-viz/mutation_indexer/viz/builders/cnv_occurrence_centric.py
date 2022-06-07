import logging

from pyspark.sql import SQLContext
from pyspark.sql.functions import struct

from mutation_indexer.core import configuration
from mutation_indexer.core.constants import logging as logging_constants
from mutation_indexer.driver.builders import bases
from mutation_indexer.viz import builders
from mutation_indexer.viz.builders import df_builders

logging.basicConfig(format=logging_constants.LOG_FORMAT)


class CNVOccurrenceCentricBuilder(bases.Builder):
    """
    Builds cnv-occurrence-centric dataframe given
    case, gene, and maf dataframes:

    cnv_occurrence{}
        |
        |____ case{}
        |       |____ observation[]
        |
        |____ cnv{}
                |____ consequence[]
                            |_____ gene{}
    """

    index_name = "cnv_occurrence_centric"
    id_field = "cnv_occurrence_id"

    def __init__(
        self,
        config: configuration.ConfigAdapter,
        sqlContext: SQLContext,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(self, ascat_df, case_df):
        """
        Builds CNV Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "read":
            self.cnv_occurrence_centric = self.load_raw()
            if self.cnv_occurrence_centric is not None:
                return self

        self.log_count(ascat_df)

        # CNV subtree
        cnv_df = self.build_cnv_subtree(ascat_df)

        # Case subtree
        case_subtree = self.build_case_subtree(ascat_df, case_df)

        self.log("Joining cnv with case")

        cnv_occurrence_centric = (
            cnv_df.join(case_subtree, on=["case_id", "cnv_id"], how="inner")
            .withColumnRenamed("occurrence_id", "cnv_occurrence_id")
            .drop("case_id")
            .drop("cnv_id")
        )

        self.log_count(cnv_occurrence_centric)

        self.cnv_occurrence_centric = cnv_occurrence_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_cnv_subtree(self, ascat_df):
        """
        cnv{}
            |____ consequence[]
                        |_____ gene{}
        """

        # Consequence
        cons_df = self.consequence_builder.build_for_cnv(ascat_df, self.index_name)

        cnv_df = df_builders.build_cnv_subtree(
            ascat_df, self.index_name, cons_df=cons_df, add_fields=["case_id"]
        )

        cnv_subtree = cnv_df.select(
            "cnv_id",
            "case_id",
            struct(
                "consequence", *cnv_df.drop("consequence").drop("case_id").columns
            ).alias("cnv"),
        )

        return cnv_subtree

    def build_case_subtree(self, ascat_df, case_df):
        """
        case{}
            |____ observation[]
        """
        self.log("Building case subtree")

        # Observation
        obs_df = self.observation_builder.build_for_cnv(ascat_df, self.index_name)

        self.log("Join observation with case")
        case_obs_df = case_df.join(obs_df, on="case_id", how="left").select(
            "case_id",
            "occurrence_id",
            "cnv_id",
            struct("observation", *case_df.columns).alias("case"),
        )
        self.log_count(case_obs_df)
        return case_obs_df
