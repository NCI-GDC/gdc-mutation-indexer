from typing import Optional

from pyspark import sql

from mutation_indexer.core import configuration
from mutation_indexer.driver.builders import bases


class GeneExpressionBuilder(bases.Builder):
    """
    A builder class for loading gene expression data.
    """

    index_name = "gene_expression"
    # NOTE: We might need a synthetic ID here, when we add support for Aliquot
    #   level gene expressions
    id_field = "case_id"

    def __init__(self, config: configuration.ConfigAdapter, sqlContext: sql.SQLContext):
        super().__init__(config, sqlContext)

        self.gene_expression: Optional[sql.DataFrame] = None
        self.gene_expression_backup = "neither"

    def build(
        self, case_df: sql.DataFrame, ge_values_df: sql.DataFrame
    ) -> "GeneExpressionBuilder":
        """
        Combines the ge case data and the ge expression value data based on the
        file they are associated with.

        Args:
            case_df: the output of the GeneExpressionCaseInputBuilder
            ge_values_df: the output of the GeneExpressionValueInputBuilder

        Returns:
            The finalized Gene Expression Data Frame

            gene_expression {}
            |---age_at_diagnosis
            |---case_id
            |---days_to_death
            |---ethnicity
            |---gender
            |---genes [{}]
            |   |---expression_value
            |   |---gene_id
            |   +---symbol
            |---project_id
            |---race
            |---submitter_id
            +---vital_status
        """

        # NOTE: the default join strategy is 'inner', so any extra cases/expression
        #   values will be dropped, which is expected
        self.gene_expression = case_df.join(ge_values_df, "file_id").select(
            "age_at_diagnosis",
            "case_id",
            "days_to_death",
            "ethnicity",
            "gender",
            "genes",
            "project_id",
            "race",
            "submitter_id",
            "vital_status",
        )

        return self
