import logging

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Self

from mutation_indexer.builders import base_builder
from mutation_indexer.configuration import old_adapter
from mutation_indexer.viz.builders import consequence, df_builders, observation

logging.basicConfig(format=old_adapter.LOG_FORMAT)


class SSMCentricBuilder(base_builder.BaseBuilder):
    """
    Builds ssm-centric dataframe given case and maf dataframes::

        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
    """

    index_name = "ssm_centric"
    id_field = "ssm_id"

    def __init__(
        self,
        config: old_adapter.BaseConfig,
        sqlContext: sql.SQLContext,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
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
        Builds SSM Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "read":
            self.ssm_centric = self.load_raw()
            if self.ssm_centric is not None:
                return self

        ssm_df = df_builders.get_ssm_df(
            maf_df, self.index_name, unique_fields=["ssm_id"]
        )

        cons_df = self.build_consequence(maf_df)

        occurrence_df = self.build_occurrence(maf_df, case_df, primary_aliquot_df)

        self.log("Final join SSM + Consequence + Occurrence")
        ssm_centric = ssm_df.join(cons_df, on="ssm_id").join(occurrence_df, on="ssm_id")

        # Truncate outliers
        treshold = self.config.percentile_threshold["occurrences_per_ssm"]
        self.ssm_centric = self.truncate_df_at_percentile(
            ssm_centric, "occurrence", treshold
        )
        self.log_count(self.ssm_centric)
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_consequence(self, maf_df):
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df, self.index_name, join_gene=True, add_gene_aa_change=True
        )

        return cons_df

    def build_occurrence(
        self,
        maf_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        # Observation
        self.log("Aggregating Observation from MAF")
        obs_df = self.observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self.index_name,
        )

        self.log("Joining Cases with Observation, [right, case_id]")
        occurrence_df = (
            case_df.join(obs_df, on=["case_id"], how="right")
            .select(
                "ssm_id",
                F.struct(
                    "occurrence_id",
                    F.struct("observation", *case_df.columns).alias("case"),
                ).alias("occurrence"),
            )
            .groupby("ssm_id")
            .agg(F.collect_list("occurrence").alias("occurrence"))
        )
        self.log_count(occurrence_df)

        return occurrence_df
