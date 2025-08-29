from collections.abc import Iterable
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import builders, indexd_utils, schemas
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import configuration


class ExpressionValueInputs(TypedDict):
    gene_model_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class ExpressionValueBuilder(
    builders.InputBuilder[configuration.ExpressionValueBuilder, ExpressionValueInputs]
):
    __slots__ = "_doc_dataframe_util"

    def __init__(
        self,
        config: configuration.ExpressionValueBuilder,
        spark_session: sql.SparkSession,
        doc_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=ExpressionValueInputs,
            output=build.DataFrame.EXPRESSION_VALUE,
        )

        self._doc_dataframe_util = doc_dataframe_util

    def _load_expression_values(self, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
        """
        Load the gene expression data from the files referenced in the primary aliquot df

        Args:
            primary_aliquot_df: The dataframe of primary aliquot data for all
                "STAR - Counts files"

        Returns:
            A data frame with the values from the files contained within the primary
            aliquot data frame.

            value {}
            |---file_id
            |---gene_id
            |---symbol
            +---uqfpkm
        """
        file_ids: Iterable[str] = (
            row.file_id
            for row in primary_aliquot_df.select("file_id").distinct().toLocalIterator()
        )
        schema = schemas.load_schema("builders/gene_expression/star_counts.json")
        gene_expression_df = self._doc_dataframe_util.get_dataframe(
            file_ids, schema=schema, comment="#", has_header=True
        ).where(F.col("gene_type") == F.lit("protein_coding"))

        return gene_expression_df.select(
            F.col("did").alias("file_id"),
            F.element_at(F.split("gene_id", "\\."), 1).alias("gene_id"),
            F.col("gene_name").alias("symbol"),
            F.col("fpkm_uq_unstranded").alias("uqfpkm"),
        )

    def _build_from_scratch(self, input_dfs: ExpressionValueInputs) -> sql.DataFrame:
        """
        Creates a data frame with the final gene expression data as found in the
        appropriate data files in indexd. Also adds the calculated log2 value of the
        fpkm_uq_unstranded value.

        Args:
            gene_model_df: The output of the GeneModelBuilder.
            gene_expression_primary_aliquot_df: the output of the
                gene_expression.PrimaryAliquotBuilder

        Returns:
            a data frame of gene expression objects to be loaded into the index.

            gene_expression {}
            |---case_id
            |---gene_expression_id
            |---gene_id
            |---log2_uqfpkm
            |---submitter_id
            |---symbol
            +---uqfpkm
        """
        gene_model_df = input_dfs["gene_model_df"]
        primary_aliquot_df = input_dfs["primary_aliquot_df"]

        gene_model_df = (
            gene_model_df.where(utils.is_protein_coding())
            .where(utils.is_between_chr1_and_chr22())
            .select(F.col("_gene_id").alias("gene_id"))
        )

        values_df = self._load_expression_values(primary_aliquot_df)
        # Remove sex chromosomes
        values_df = values_df.join(gene_model_df, on=["gene_id"], how="inner")

        gene_expression_df = values_df.join(
            primary_aliquot_df, on=["file_id"], how="inner"
        ).select(
            "*",
            utils.uuid5_col("case_id", "gene_id").alias("gene_expression_id"),
            F.log2(F.col("uqfpkm") + 1).cast(types.FloatType()).alias("log2_uqfpkm"),
        )

        # Keep only the columns we need,
        # repartition by gene_id so each partition has all cases for only one gene,
        # and sort each partition by case_id in ascending order.
        # The backup parquet file will have as many partitions as genes are and each
        # partition will have all cases ordered.
        return gene_expression_df.select(
            "case_id",
            "gene_expression_id",
            "gene_id",
            "log2_uqfpkm",
            "submitter_id",
            "symbol",
            "uqfpkm",
        )
