from typing import TypedDict

from pyspark import sql

from mutation_indexer import indexd_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


class SegmentCNVInputs(TypedDict):
    segment_cnv_metadata_df: sql.DataFrame


class SegmentCNVBuilder(bases.InputBuilder[viz.Builder, SegmentCNVInputs]):
    def __init__(
        self,
        config: viz.Builder,
        spark_session: sql.SparkSession,
        document_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        """Input dataframe builder that gathers copy number segment file data.

        Given the copy number segment file ids as input, the file contents are retrieved
        from indexd. The segment length is calculated using the start and end positions
        of the chromosome, and then the cnv_change and cnv_change_5_category fields are
        calculated using a weighted mode.
        """
        super().__init__(
            config,
            spark_session,
            input_type=SegmentCNVInputs,
            output=build.DataFrame.SEGMENT_CNV,
        )

        self._document_dataframe_util = document_dataframe_util

    def _build_from_scratch(self, input_dfs: SegmentCNVInputs) -> sql.DataFrame:
        """Builds the SegmentCNV dataframe.

        segment_cnv {}
        |---segment_cnv_id
        |---occurrence_id
        |---observation_id
        |---chromosome
        |---length
        |---start_position
        |---end_position
        |---cnv_change
        |---cnv_change_5_category

        """
        segment_cnv_metadata_df = input_dfs["segment_cnv_metadata_df"]

        return segment_cnv_metadata_df
