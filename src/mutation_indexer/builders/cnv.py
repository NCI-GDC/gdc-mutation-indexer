import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import bases, df_builders, utils
from mutation_indexer.configuration.builders import viz
from mutation_indexer.configuration.builders.common import IndexBuilder
from mutation_indexer.constants import app, build

logging.basicConfig(format=app.LOG_FORMAT)


class CNVCentricInputs(TypedDict):
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame


class CNVCentricBuilder(bases.IndexBuilder[viz.CNVCentricBuilder, CNVCentricInputs]):
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

    __slots__ = ("_consequence_builder", "_observation_builder")

    def __init__(
        self,
        config: viz.CNVCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=CNVCentricInputs,
            output=build.DataFrame.CNV_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: CNVCentricInputs) -> sql.DataFrame:
        ascat_df = input_dfs["ascat_df"]
        case_df = input_dfs["case_df"]
        cnv_df = df_builders.get_cnv_df(ascat_df, self._index_name)
        consequence_df = self._consequence_builder.build_for_cnv(
            ascat_df, self._index_name
        )
        occurrence_df = self._build_occurrence_df(ascat_df, case_df)
        cnv_centric_df = cnv_df.join(consequence_df, on="cnv_id", how="left").join(
            occurrence_df, on="cnv_id", how="left"
        )
        cnv_centric_df = utils.filter_arrays_by_relative_size(
            cnv_centric_df, "occurrence", self._config.occurrences_threshold
        )

        return cnv_centric_df

    def _build_occurrence_df(
        self, ascat_df: sql.DataFrame, case_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Assumes you've already added 'case_id'

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        observation_df = self._observation_builder.build_for_cnv(
            ascat_df,
            self._index_name,
        )
        occurrence_df = (
            case_df.join(observation_df, on=["case_id"], how="left")
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

        return occurrence_df


class CNVOccurrenceCentricInputs(TypedDict):
    cnv_centric_df: sql.DataFrame


class CNVOccurrenceCentricBuilder(
    bases.IndexBuilder[viz.IndexBuilder, CNVOccurrenceCentricInputs]
):
    def __init__(
        self,
        config: IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=CNVOccurrenceCentricInputs,
            output=build.DataFrame.CNV_OCCURRENCE_CENTRIC,
        )

    def _build_from_scratch(
        self, input_dfs: CNVOccurrenceCentricInputs
    ) -> sql.DataFrame:
        cnv_occurrence_centric_df = (
            input_dfs["cnv_centric_df"]
            .select(
                F.struct(
                    "chromosome",
                    "cnv_change",
                    "cnv_id",
                    "consequence",
                    "end_position",
                    "gene_level_cn",
                    "ncbi_build",
                    "start_position",
                    F.lit("tumor only").alias("variant_status"),
                ).alias("cnv"),
                F.explode("occurrence").alias("occurrence"),
            )
            .select("occurrence.case", "cnv", "occurrence.cnv_occurrence_id")
        )

        return cnv_occurrence_centric_df
