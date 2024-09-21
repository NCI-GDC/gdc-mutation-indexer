from collections.abc import Sequence
from typing import TypedDict

from pyspark import sql

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build


def _get_primary_aliquot_filters(projects: Sequence[str]) -> list[dict]:
    filters: list[dict] = [
        {"terms": {"data_type": ["Gene Expression Quantification"]}},
        {"terms": {"acl": ["open"]}},
        {"term": {"analysis.workflow_type": "STAR - Counts"}},
    ]

    if projects:
        project_filter = {
            "nested": {
                "path": "cases",
                "query": {"terms": {"cases.project.project_id": projects}},
            }
        }

        filters.append(project_filter)

    return filters


class STARCountMetadataInputs(TypedDict):
    pass


class STARCountMetadataBuilder(
    bases.PrimaryAliquotBuilder[gene_expression.Builder, STARCountMetadataInputs]
):
    def __init__(
        self,
        config: gene_expression.Builder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util=es_dataframe_util,
            input_type=STARCountMetadataInputs,
            output=build.DataFrame.STAR_COUNT_METADATA,
        )

    def _build_from_scratch(self, input_dfs: STARCountMetadataInputs) -> sql.DataFrame:
        """
        Gets the case and it's associated file data for the mutation index.

        Args:
            workflow_types: A collection of analysis workflow types to
                filter files on.

        Returns:
            (GeneExpressionPrimaryAliquotData): An object containing the primary aliquot
            dataframe as well as a list of all the file urls associated with the primary
            aliquots.

            primary_aliquot {}
            |---file_id
            |---case_id
            +---submitter_id
        """
        filters = _get_primary_aliquot_filters(self._config.projects)
        case_fields = [
            "cases.submitter_id",
        ]

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=case_fields,
        ).select(
            "file_id",
            "case_id",
            "case.submitter_id",
        )
