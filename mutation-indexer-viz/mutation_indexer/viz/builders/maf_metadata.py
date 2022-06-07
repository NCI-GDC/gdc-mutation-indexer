from typing import Any, Dict, Iterable, List, Union

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal

from mutation_indexer.core import configuration
from mutation_indexer.driver import es_utils
from mutation_indexer.driver.builders import bases


class MAFMetadataBuilder(bases.PrimaryAliquotBuilder):
    """
    An input builder for collecting the metadata associated with the MAF
    data that will be loaded as part of the build process.
    """

    def __init__(
        self,
        config: configuration.ConfigAdapter,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
    ):
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch
        """
        super().__init__(
            config,
            sqlContext,
            es_dataframe_util,
            input_type="maf_metadata",
            additional_selections=("data_type", "workflow_type"),
        )

    def _build_file_filters(self) -> List[Dict[str, Any]]:
        aesvmm_workflow = {
            "bool": {
                "must": [
                    {"term": {"data_format": "MAF"}},
                    {"term": {"data_type": "Masked Somatic Mutation"}},
                    {
                        "term": {
                            "analysis.workflow_type": "Aliquot Ensemble Somatic Variant Merging and Masking"
                        }
                    },
                ]
            }
        }
        fvam_workflow = {
            "bool": {
                "must": [
                    {"term": {"data_format": "MAF"}},
                    {"term": {"data_type": "Aggregated Somatic Mutation"}},
                    {
                        "term": {
                            "analysis.workflow_type": "FoundationOne Variant Aggregation and Masking"
                        }
                    },
                ]
            }
        }
        filters: List[dict] = [{"bool": {"should": [aesvmm_workflow, fvam_workflow]}}]

        if self.config.projects:
            filters.append(
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {
                                "cases.project.project_id": list(self.config.projects)
                            }
                        },
                    }
                },
            )

        return filters

    def _get_initial_weighted_df(
        self, query: dict, include_fields: Union[Iterable[str], Literal[True]]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select("*", F.col("analysis.workflow_type").alias("workflow_type"))
        )

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the maf file data (file_id, workflow_type, and data_type) and its
        associated case id.

        Returns:
            The below data frame

            maf_metadata{}
            |---case_id
            |---data_type
            |---file_id
            +---workflow_type
        """
        filters = self._build_file_filters()

        return self._get_primary_aliquot_df(
            filters,
            frozenset(("case",)),
            include_fields=("data_type", "analysis.workflow_type"),
        ).select(
            "case_id",
            "data_type",
            "file_id",
            "workflow_type",
        )
