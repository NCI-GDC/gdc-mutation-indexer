from collections.abc import Collection
from typing import Sequence, TypedDict, Union

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal, override

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


class ASCATMetadataInputs(TypedDict):
    pass


class ASCATMetadataBuilder(
    bases.InclusivePrimaryAliquotBuilder[viz.Builder, ASCATMetadataInputs]
):
    """
    A class for resolving the document IDs associated with the ASCAT documents in
    ES/Indexd.
    """

    def __init__(
        self,
        config: viz.Builder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            es_rdd_util,
            additional_selections=("workflow_type", "analysis_id"),
            input_type=ASCATMetadataInputs,
            output=build.DataFrame.ASCAT_METADATA,
        )

    @override
    def _weight_matrix(self) -> Sequence[Sequence[sql.Column]]:
        workflow_type = F.col("workflow_type")
        file_weights = (
            workflow_type == F.lit(build.WorkflowType.ABSOLUTE.value),
            workflow_type == F.lit(build.WorkflowType.ASCAT3.value),
            workflow_type == F.lit(build.WorkflowType.ASCAT_NGS.value),
            workflow_type == F.lit(build.WorkflowType.ASCAT2.value),
        )

        # apply file weights as a higher order weight to the defaults.
        return (*super()._weight_matrix(), file_weights)

    @override
    def _get_initial_weighted_df(
        self, query: dict, include_fields: Union[Collection[str], Literal[True]]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select(
                "*",
                F.col("analysis.workflow_type").alias("workflow_type"),
                F.col("analysis.analysis_id").alias("analysis_id"),
            )
        )

    def _get_filters(self) -> list[dict]:
        return [
            {
                "bool": {
                    "must": [
                        {"term": {"data_type": "Gene Level Copy Number"}},
                        {"terms": {"acl": self._config.acl}},
                    ],
                    "minimum_should_match": 1,
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "experimental_strategy": "Genotyping Array"
                                        }
                                    },
                                    {
                                        "term": {
                                            "analysis.workflow_type": build.WorkflowType.ABSOLUTE.value
                                        }
                                    },
                                ]
                            },
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "experimental_strategy": "Genotyping Array"
                                        }
                                    },
                                    {
                                        "term": {
                                            "analysis.workflow_type": build.WorkflowType.ASCAT3.value
                                        }
                                    },
                                ]
                            },
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"experimental_strategy": "WGS"}},
                                    {
                                        "term": {
                                            "analysis.workflow_type": build.WorkflowType.ASCAT_NGS.value
                                        }
                                    },
                                ]
                            },
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "experimental_strategy": "Genotyping Array"
                                        }
                                    },
                                    {
                                        "term": {
                                            "analysis.workflow_type": build.WorkflowType.ASCAT2.value
                                        }
                                    },
                                ]
                            },
                        },
                    ],
                }
            }
        ]

    def _build_from_scratch(self, input_dfs: ASCATMetadataInputs) -> sql.DataFrame:
        filters = self._get_filters()

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=("analysis.workflow_type", "analysis.analysis_id"),
        ).select("aliquot_id", "case_id", "file_id", "workflow_type", "analysis_id")
