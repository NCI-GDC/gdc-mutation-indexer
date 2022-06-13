from typing import List, Optional

from pyspark import sql

from mutation_indexer.core import configuration
from mutation_indexer.driver import es_utils
from mutation_indexer.driver.builders import bases


def _get_gene_expression_filters(projects: Optional[List[str]]) -> List[dict]:
    filters = [
        {"terms": {"data_type": ["Gene Expression Quantification"]}},
        {"terms": {"acl": ["open"]}},
        {"term": {"analysis.workflow_type": "STAR - Counts"}},
    ]  # type: List[dict]

    if projects:
        filters.append(
            {
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": projects}},
                }
            }
        )

    return filters


class PrimaryAliquotBuilder(bases.PrimaryAliquotBuilder):
    def __init__(
        self,
        config: configuration.ConfigAdapter,
        sql_context: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
        """
        super().__init__(
            config, sql_context, es_dataframe_util, "gene_expression_primary_aliquot"
        )

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the case and it's associated file data for the mutation index.

        Args:
            workflow_types: A collection of analysis workflow types to
                filter files on.

        Returns:
            (GeneExpressionPrimaryAliquotData): An object containing the primary aliquot
            dataframe as well as a list of all the file urls associated with the primary
            aliquots.

            primary_aliquot{}
            |---file_id
            |---file_url
            |---case_id
            |---submitter_id
            |---demographic{}
            |   |---days_to_death
            |   |---ethnicity
            |   |---gender
            |   |---race
            |   |---vital_status
            |
            |---project{}
            |   |---project_id
            |
            |---diagnoses[]
            |   |---age_at_diagnosis
            |
            |---samples[]
                |---sample_type
        """
        filters = _get_gene_expression_filters(self.config.projects)
        case_fields = [
            "cases.submitter_id",
            "cases.demographic.days_to_death",
            "cases.demographic.ethnicity",
            "cases.demographic.gender",
            "cases.demographic.race",
            "cases.demographic.vital_status",
            "cases.project.project_id",
            "cases.diagnoses.age_at_diagnosis",
        ]

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=case_fields,
        ).select(
            "file_id",
            "case_id",
            "case.submitter_id",
            "case.demographic",
            "case.project",
            "case.diagnoses",
            "case.samples",
        )
