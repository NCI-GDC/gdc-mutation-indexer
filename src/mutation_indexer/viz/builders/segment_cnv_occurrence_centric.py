"""Builds the segment_cnv_occurrence_centric dataframe."""

from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import observation


class SegmentCNVOccurrenceCentricBuilderInputs(TypedDict):
    segment_cnv_df: sql.DataFrame
    case_df: sql.DataFrame


class SegmentCNVOccurrenceCentricBuilder(
    builders.IndexBuilder[
        configuration.SegmentCNVOccurrenceCentricBuilder,
        SegmentCNVOccurrenceCentricBuilderInputs,
    ]
):
    def __init__(
        self,
        config: configuration.SegmentCNVOccurrenceCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ):
        """
        Builds segment_cnv_occurrence_centric dataframe given case and segment_cnv
        dataframes.

        segment_cnv_occurrence{}
            |____ case{}
                    |____ observation[]
            |____ segment_cnv{}
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=SegmentCNVOccurrenceCentricBuilderInputs,
            output=build.DataFrame.SEGMENT_CNV_OCCURRENCE_CENTRIC,
        )

    def _build_segment_cnv_column(self, segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
        """Builds the segment_cnv column that will end up in the final dataframe.

        segment_cnv{}
        |____ segment_cnv_id
        |____ case_id
        |____ segment_cnv{}
                |____ segment_cnv_id
                |____ chromosome
                |____ length
                |____ start_position
                |____ end_position
                |____ end_position
                |____ cnv_change
                |____ cnv_change_5_category

        The root level fields segment_cnv_id and case_id are required for future
        joining with the case_column_df (see _build_case_column).

        Duplicate rows are dropped based on the segment_cnv_id and case_id fields.
        This will ensure that the final dataframe will have rows with unique
        segment_cnv_id and case_id combinations.
        """
        segment_columns = (
            "segment_cnv_id",
            "chromosome",
            "length",
            "start_position",
            "end_position",
            "cnv_change",
            "cnv_change_5_category",
        )
        segment_column_df = segment_cnv_df.select(
            "segment_cnv_id",
            "case_id",
            F.struct(*segment_columns).alias("segment_cnv"),
        )
        segment_column_df = segment_column_df.drop_duplicates(
            subset=["segment_cnv_id", "case_id"]
        )

        return segment_column_df

    def _build_case_column(
        self, segment_cnv_df: sql.DataFrame, case_df: sql.DataFrame
    ) -> sql.DataFrame:
        """Builds the case column that will end up in the final dataframe.

        case{}
        |____ observation [{}] (see build_observation_for_segment_cnv)
        |____ segment_cnv_id
        |____ case_id

        The root level fields segment_cnv_id and case_id are required for future
        joining with the segment_cnv_column_df (see _build_segment_cnv_column),
        and the occurrence_id will become the eventual segment_cnv_occurrence_id.
        """
        obs_df = observation.build_observation_for_segment_cnv(segment_cnv_df)
        case_column_df = case_df.join(obs_df, on="case_id", how="left").select(
            "segment_cnv_id",
            "case_id",
            "occurrence_id",
            F.struct("observation", *case_df.columns).alias("case"),
        )

        return case_column_df

    def _build_segment_cnv_occurrence_centric(
        self, segment_cnv_column_df: sql.DataFrame, case_column_df: sql.DataFrame
    ) -> sql.DataFrame:
        """Builds the segment_cnv_occurrence_centric dataframe.

        This will rename occurrence_id as the segment_cnv_occurrence_id.
        """
        segment_cnv_occurrence_centric_df = segment_cnv_column_df.join(
            case_column_df, on=["segment_cnv_id", "case_id"], how="inner"
        )
        segment_cnv_occurrence_centric_df = segment_cnv_occurrence_centric_df.select(
            F.col("occurrence_id").alias("segment_cnv_occurrence_id"),
            "segment_cnv",
            "case",
        )

        return segment_cnv_occurrence_centric_df

    def _build_from_scratch(
        self, input_dfs: SegmentCNVOccurrenceCentricBuilderInputs
    ) -> sql.DataFrame:
        """Builds segment_cnv_occurrence_centric dataframe.

        STEPS:
            1) Build the dataframe that will populate the segment_cnv struct
            column in the final dataframe. The duplicate rows (by segment_cnv_id and
            case_id) will be dropped to avoid extraneous work during step 3.

            2) Build the dataframe that will populate the case struct column in
            the final dataframe.

            3) Join the above two dataframes.
        """
        segment_cnv_df = input_dfs["segment_cnv_df"]
        case_df = input_dfs["case_df"]

        segment_cnv_column_df = self._build_segment_cnv_column(segment_cnv_df)
        case_column_df = self._build_case_column(segment_cnv_df, case_df)
        segment_cnv_occurrence_centric_df = self._build_segment_cnv_occurrence_centric(
            segment_cnv_column_df, case_column_df
        )

        return segment_cnv_occurrence_centric_df
