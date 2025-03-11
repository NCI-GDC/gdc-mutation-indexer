from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build

DEPRECATED_DATA_TYPE = "Copy Number Segment"
DATA_TYPE = "Allele-specific Copy Number Segment"
ES_QUERY_TYPE = dict[str, dict[str, dict[str, list]]]


class SegmentCNVMetadataInputs(TypedDict):
    ascat_metadata_df: sql.DataFrame


class SegmentCNVMetadataBuilder(
    bases.InputBuilder[viz.SegmentCNVMetadataBuilder, SegmentCNVMetadataInputs]
):
    def __init__(
        self,
        config: viz.SegmentCNVMetadataBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """Input dataframe builder that retrieves copy number segment files.

        Then, it uses the output of ASCATMetadataBuilder and joins the copy number
        segment files with the primary aliquot on analysis_id. This ensures that the
        copy number segment file will be the sibling file of the gene-level copy
        number file.
        """
        super().__init__(
            config,
            spark_session,
            input_type=SegmentCNVMetadataInputs,
            output=build.DataFrame.SEGMENT_CNV_METADATA,
        )

        self._es_dataframe_util = es_dataframe_util

    def _get_es_query(self) -> dict:
        query: ES_QUERY_TYPE = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"acl": self._config.acl}},
                    ]
                }
            }
        }
        additional_filters = (
            [
                {"term": {"data_type": DEPRECATED_DATA_TYPE}},
                {"term": {"analysis.workflow_type": build.WorkflowType.ASCAT_NGS}},
            ]
            if self._config.use_deprecated_query is True
            else [{"term": {"data_type": DATA_TYPE}}]
        )
        query["query"]["bool"]["must"].extend(additional_filters)

        return query

    def _get_es_source_fields(self) -> tuple[str, ...]:
        return ("file_id", "analysis.analysis_id")

    def _build_from_scratch(self, input_dfs: SegmentCNVMetadataInputs) -> sql.DataFrame:
        """Builds the SegmentCNVMetadata dataframe.

        segment_cnv_metadata {}
        |---aliquot_id
        |---analysis_id
        |---case_id
        |---file_id
        |---workflow_type
        """
        ascat_metadata_df = input_dfs["ascat_metadata_df"].select(
            "aliquot_id", "analysis_id", "case_id", "workflow_type"
        )
        segment_cnv_metadata_df = self._es_dataframe_util.read(
            build.IndexType.FILE,
            source_filter=self._get_es_source_fields(),
            query=self._get_es_query(),
        )
        segment_cnv_metadata_df = segment_cnv_metadata_df.select(
            "file_id", F.col("analysis.analysis_id").alias("analysis_id")
        )
        segment_cnv_metadata_df = ascat_metadata_df.join(
            segment_cnv_metadata_df, on="analysis_id", how="inner"
        )
        segment_cnv_metadata_df = segment_cnv_metadata_df.select(
            "aliquot_id", "analysis_id", "case_id", "file_id", "workflow_type"
        )

        return segment_cnv_metadata_df
