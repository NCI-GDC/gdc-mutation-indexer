from typing import Iterable

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.core import configuration
from mutation_indexer.driver import indexd_utils
from mutation_indexer.driver.builders import bases
from mutation_indexer.gene_expression import schemas


class ValueBuilder(bases.InputBuilder):
    """
    An input builder class for loading gene expression values.
    """

    def __init__(
        self,
        config: configuration.ConfigAdapter,
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
