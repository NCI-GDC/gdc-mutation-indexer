from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build

DATA_TYPE = "Copy Number Segment"


class SegmentCnvMetadataInputs(TypedDict):
    ascat_metadata_df: sql.DataFrame


class SegmentCnvMetadataBuilder(
    bases.InputBuilder[viz.Builder, SegmentCnvMetadataInputs]
):
    """Input dataframe builder that retrieves copy number segment files.

    Then, it uses the output of ASCATMetadataBuilder and joins the copy number segment
    files with the primary aliquot on analysis_id. This ensures that the copy number
    segment file will be the sibling file of the gene-level copy number file.
    """

    def __init__(
        self,
        config: viz.Builder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=SegmentCnvMetadataInputs,
            output=build.DataFrame.SEGMENT_CNV_METADATA,
        )

        self._es_dataframe_util = es_dataframe_util

    def _get_es_query(self) -> dict:
        # TODO: DEV-3243
        # Change the following query to query for:
        #   data_type == "Allele-specific Copy Number Segment"
        # instead of the current data_type and workflow_type filters
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"data_type": DATA_TYPE}},
                        {
                            "term": {
                                "analysis.workflow_type": build.WorkflowType.ASCAT_NGS
                            }
                        },
                        {"terms": {"acl": self._config.acl}},
                    ]
                }
            }
        }

    def _get_es_source_fields(self) -> list[str]:
        return ["file_id", "analysis.analysis_id"]

    def _build_from_scratch(self, input_dfs: SegmentCnvMetadataInputs) -> sql.DataFrame:
        ascat_metadata_df = input_dfs["ascat_metadata_df"]
        segment_cnv_df = self._es_dataframe_util.read(
            build.IndexType.FILE,
            source_filter=self._get_es_source_fields(),
            query=self._get_es_query(),
        )
        segment_cnv_df = segment_cnv_df.select(
            "file_id", F.col("analysis.analysis_id").alias("analysis_id")
        )
        segment_cnv_df = ascat_metadata_df.join(
            segment_cnv_df, on="analysis_id", how="inner"
        ).select(
            ascat_metadata_df.aliquot_id,
            segment_cnv_df.analysis_id,
            ascat_metadata_df.case_id,
            segment_cnv_df.file_id,
            ascat_metadata_df.workflow_type,
        )

        return segment_cnv_df
