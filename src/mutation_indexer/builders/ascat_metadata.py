from collections.abc import Collection
from typing import TypedDict, Union

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal, override

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build

ABSOLUTE = "ABSOLUTE LiftOver"
ASCAT3 = "ASCAT3"
ASCAT2 = "ASCAT2"
ASCAT_NGS = "AscatNGS"


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
            additional_selections=("workflow_type",),
            input_type=ASCATMetadataInputs,
            output=build.DataFrame.ASCAT_METADATA,
        )

    @override
    def _weight_matrix(self) -> bases.Matrix:
        dimension = bases.MatrixDimension(
            "workflow_type", (ABSOLUTE, ASCAT3, ASCAT_NGS, ASCAT2)
        )

        return super()._weight_matrix().prepend(dimension)

    @override
    def _get_initial_weighted_df(
        self, query: dict, include_fields: Union[Collection[str], Literal[True]]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select("*", F.col("analysis.workflow_type").alias("workflow_type"))
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
                                    {"term": {"analysis.workflow_type": ABSOLUTE}},
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
                                    {"term": {"analysis.workflow_type": ASCAT3}},
                                ]
                            },
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"experimental_strategy": "WGS"}},
                                    {"term": {"analysis.workflow_type": ASCAT_NGS}},
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
                                    {"term": {"analysis.workflow_type": ASCAT2}},
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
            include_fields=("analysis.workflow_type",),
        ).select("aliquot_id", "case_id", "file_id", "workflow_type")
