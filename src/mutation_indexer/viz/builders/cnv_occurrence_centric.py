from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import consequence, df_builders, observation


class Inputs(TypedDict):
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame


class CNVOccurrenceCentricBuilder(
    builders.IndexBuilder[configuration.CNVOccurrenceCentricBuilder, Inputs]
):
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

    def __init__(
        self,
        config: configuration.CNVOccurrenceCentricBuilder,
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
            output=build.DataFrame.CNV_OCCURRENCE_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds CNV Occurrence Centric index
        """
        ascat_df = input_dfs["ascat_df"]
        cnv_df = self._build_cnv_subtree(ascat_df)
        case_df = self._build_case_subtree(ascat_df, input_dfs["case_df"])

        return (
            cnv_df.join(case_df, on=["case_id", "cnv_id"], how="inner")
            .withColumnRenamed("occurrence_id", "cnv_occurrence_id")
            .drop("case_id")
            .drop("cnv_id")
        )

    def _build_cnv_subtree(self, ascat_df):
        """
        cnv{}
            |____ consequence[]
                        |_____ gene{}
        """

        # Consequence
        cons_df = self._consequence_builder.build_for_cnv(ascat_df, self._index_name)

        cnv_df = df_builders.build_cnv_subtree(
            ascat_df, self._index_name, cons_df=cons_df, add_fields=["case_id"]
        )

        cnv_subtree = cnv_df.select(
            "cnv_id",
            "case_id",
            F.struct("consequence", *cnv_df.drop("consequence").drop("case_id").columns).alias(
                "cnv"
            ),
        )

        return cnv_subtree

    def _build_case_subtree(self, ascat_df, case_df):
        """
        case{}
            |____ observation[]
        """
        obs_df = self._observation_builder.build_for_cnv(ascat_df, self._index_name)

        return case_df.join(obs_df, on="case_id", how="left").select(
            "case_id",
            "occurrence_id",
            "cnv_id",
            F.struct("observation", *case_df.columns).alias("case"),
        )
