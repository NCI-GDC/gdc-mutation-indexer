from typing import TypedDict

from pyspark import sql

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
            input_type=ASCATMetadataInputs,
            output=build.DataFrame.ASCAT_METADATA,
        )

    def _build_from_scratch(self, input_dfs: ASCATMetadataInputs) -> sql.DataFrame:
        project_clause = (
            {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self._config.projects}
                    },
                }
            }
            if self._config.projects
            else {"match_all": {}}
        )
        filters = [
            {
                "bool": {
                    "must": [
                        {"term": {"data_type": "Gene Level Copy Number"}},
                        {"terms": {"acl": self._config.acl}},
                        project_clause,
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
                                    {"term": {"analysis.workflow_type": "ASCAT2"}},
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"experimental_strategy": "WGS"}},
                                    {"term": {"analysis.workflow_type": "AscatNGS"}},
                                ]
                            }
                        },
                    ],
                }
            }
        ]

        return self._get_primary_aliquot_df(
            filters, entities=frozenset(("case",))
        ).select("aliquot_id", "case_id", "file_id")
