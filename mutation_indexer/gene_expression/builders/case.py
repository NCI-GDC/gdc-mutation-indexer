from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.core import configuration
from mutation_indexer.driver.builders import bases


class CaseBuilder(bases.InputBuilder):
    """
    An input builder class for loading case data related to gene expression.
    """

    def __init__(self, config: configuration.ConfigAdapter, sqlContext: sql.SQLContext):
        super().__init__(config, sqlContext, "gene_expression_cases")

    def build_from_scratch(
        self, gene_expression_primary_aliquot_df: sql.DataFrame, **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Creates a data frame containing the case data associated with the aliquots in the ge
        primary aliquot data and their related file_id.

        Args:
            gene_expression_primary_aliquot_df: the output of the GeneExpressionPrimaryAliquotBuilder

        Returns:
            a data frame of gene expression cases

            gene_expression_case {}
            |---age_at_diagnosis
            |---case_id
            |---days_to_death
            |---ethnicity
            |---file_id
            |---gender
            |---project_id
            |---race
            |---submitter_id
            +---vital_status
        """
        initial_df = gene_expression_primary_aliquot_df

        # NOTE: diagnoses is a nested document, so we are flattening it by
        #   simply aggregating age_at_diagnosis values into an array
        flat_diagnosis_df = initial_df.select(
            "case_id", F.col("diagnoses.age_at_diagnosis").alias("age_at_diagnosis")
        )

        case_ge_df = initial_df.select(
            "case_id",
            "demographic.days_to_death",
            "demographic.ethnicity",
            "demographic.gender",
            "demographic.race",
            "demographic.vital_status",
            "submitter_id",
            "project.project_id",
            "file_id",
        ).join(flat_diagnosis_df, "case_id")

        return case_ge_df.select(
            "age_at_diagnosis",
            "case_id",
            "days_to_death",
            "ethnicity",
            "file_id",
            "gender",
            "project_id",
            "race",
            "submitter_id",
            "vital_status",
        )
