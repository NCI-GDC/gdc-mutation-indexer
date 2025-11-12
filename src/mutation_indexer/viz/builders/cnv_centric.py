from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import consequence, df_builders, observation


class Inputs(TypedDict):
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame


class CNVCentricBuilder(builders.IndexBuilder[configuration.CNVCentricBuilder, Inputs]):
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

    def __init__(
        self,
        config: configuration.CNVCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=Inputs,
            output=build.DataFrame.CNV_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds CNV Centric index
        """
        ascat_df = input_dfs["ascat_df"]
        cnv_df = df_builders.get_cnv_df(ascat_df, self._index_name)
        cons_df = self._consequence_builder.build_for_cnv(
            ascat_df,
            self._index_name,
        )
        occurrence_df = self._build_occurrence_df(ascat_df, input_dfs["case_df"])
        cnv_cons_df = cnv_df.join(cons_df, on="cnv_id", how="left")
        cnv_centric_df = cnv_cons_df.join(occurrence_df, on="cnv_id", how="left")

        # truncate outliers
        cnv_centric_df = utils.filter_arrays_by_relative_size(
            cnv_centric_df, "occurrence", self._config.occurrences_threshold
        )

        return cnv_centric_df

    def _build_occurrence_df(self, ascat_df, case_df):
        """
        Assumes you've already added 'case_id'

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        # 1. Observation
        obs_df = self._observation_builder.build_for_cnv(
            ascat_df,
            self._index_name,
        )

        # 2. Join Case to Observation and create structs
        return (
            case_df.join(obs_df, on=["case_id"], how="left")
            .select(
                "cnv_id",
                F.struct(
                    "occurrence_id",
                    F.struct("observation", *case_df.columns).alias("case"),
                ).alias("occurrence"),
            )
            .groupby("cnv_id")
            .agg(F.collect_set("occurrence").alias("occurrence"))
        )
