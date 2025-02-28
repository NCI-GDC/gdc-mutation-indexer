"""Builds the segment_cnv_occurrence_centric dataframe."""
from typing import TypedDict

from pyspark import sql

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


class SegmentCNVOccurrenceCentricBuilderInputs(TypedDict):
    segment_cnv_df: sql.DataFrame
    case_df: sql.DataFrame


class SegmentCNVOccurrenceCentricBuilder(
    bases.IndexBuilder[
        viz.SegmentCNVOccurrenceCentricBuilder, SegmentCNVOccurrenceCentricBuilderInputs
    ]
):
    def __init__(
        self,
        config: viz.SegmentCNVOccurrenceCentricBuilder,
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

    def _build_from_scratch(
        self, input_dfs: SegmentCNVOccurrenceCentricBuilderInputs
    ) -> sql.DataFrame:
        """Builds segment_cnv_occurrence_centric dataframe.

        TODO: DEV-3203 fill out implementation and docstring
        """

        return input_dfs["segment_cnv_df"]
