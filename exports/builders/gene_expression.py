from typing import Iterable, Optional

from pyspark import sql
from pyspark.sql import functions as F

import config
from exports import indexd_utils, schemas
from exports.builders import base_builder, base_input_builder
from exports.configuration.builders import gene_expression


class GeneExpressionValueInputBuilder(
    base_input_builder.BaseInputBuilder[gene_expression.Builder]
):
    """
    An input builder class for loading gene expression values.
    """

    def __init__(
        self,
        config: gene_expression.Builder,
        sqlContext: sql.SQLContext,
        doc_dataframe_util: indexd_utils.DataFrameUtil,
    ):
        super().__init__(config, sqlContext, "gene_expression_values")

        self._doc_dataframe_util = doc_dataframe_util

    def build_from_scratch(
        self,
        gene_model_df: sql.DataFrame,
        gene_expression_primary_aliquot_df: sql.DataFrame,
        **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Creates a data frame containing the gene expression values contained within
        each file of the ge primary aliquot data. This excludes any expression values
        associated with any non-protein coding genes.

        Args:
            gene_model_df: The output of the GeneModelBuilder.
            gene_expression_primary_aliquot_df: the output of the GeneExpressionPrimaryAliquotBuilder

        Returns:
            a data frame of expression values associated with the file containing them

            gene_espression_value {}
            |---file_id
            +---genes [{}]
                |---expression_value
                |---gene_id
                +---symbol
        """
        pc_genes_df = gene_model_df.filter(
            F.col("biotype") == F.lit("protein_coding")
        ).select(F.col("_gene_id").alias("gene_id"), "symbol")

        ge_values_df = self.load_gene_expression_files(
            gene_expression_primary_aliquot_df
        )

        ge_values_df = (
            ge_values_df.join(pc_genes_df, "gene_id")
            .withColumn("gene", F.struct("expression_value", "gene_id", "symbol"))
            .drop("gene_id", "expression_value", "symbol")
            .groupBy("file_id")
            .agg(F.collect_list("gene").alias("genes"))
        )

        return ge_values_df.select("file_id", "genes")

    def load_gene_expression_files(
        self, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Load the gene espression data from the files referenced in the primary aliqout df

        Args:
            primary_aliquot_df: The dataframe of primary aliquot data for all
                "STAR - Counts files"
        """
        self.logger.info("Loading gene expression files")
        file_ids: Iterable[str] = (
            row.file_id
            for row in primary_aliquot_df.select("file_id").distinct().toLocalIterator()
        )
        schema = schemas.load_schema("builders/gene_expression/star_counts.json")
        gene_expression_df = self._doc_dataframe_util.get_dataframe(
            file_ids, schema=schema, comment="#", has_header=True
        ).where(F.col("gene_type") == F.lit("protein_coding"))

        return gene_expression_df.select(
            F.element_at(F.split("gene_id", "\\."), 1).alias("gene_id"),
            F.col("fpkm_uq_unstranded").alias("expression_value"),
            F.col("did").alias("file_id"),
        )


class GeneExpressionCaseInputBuilder(
    base_input_builder.BaseInputBuilder[gene_expression.Builder]
):
    """
    An input builder class for loading case data related to gene expression.
    """

    def __init__(self, config: gene_expression.Builder, sqlContext: sql.SQLContext):
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


class GeneExpressionBuilder(base_builder.BaseBuilder):
    """
    A builder class for loading gene expression data.
    """

    index_name = "gene_expression"
    # NOTE: We might need a synthetic ID here, when we add support for Aliquot
    #   level gene expressions
    id_field = "case_id"

    def __init__(self, config: config.BaseConfig, sqlContext: sql.SQLContext):
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
