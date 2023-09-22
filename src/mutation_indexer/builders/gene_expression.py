from collections.abc import Iterable, Sequence
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F, types

from mutation_indexer import es_utils, indexd_utils, schemas
from mutation_indexer.builders import bases, utils
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


class PrimaryAliquotInputs(TypedDict):
    pass


class PrimaryAliquotBuilder(
    bases.PrimaryAliquotBuilder[gene_expression.Builder, PrimaryAliquotInputs]
):
    def __init__(
        self,
        config: gene_expression.Builder,
        spark_session: sql.SparkSession,
        es_rdd_util: es_utils.RDDUtil,
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
            es_rdd_util,
            input_type=PrimaryAliquotInputs,
            output=build.DataFrame.PRIMARY_ALIQUOT,
        )

    def _load_file_schema(self) -> types.StructType:
        schema = super()._load_file_schema()
        cases = schema["cases"]

        assert isinstance(cases.dataType, types.ArrayType) and isinstance(
            cases.dataType.elementType, types.StructType
        )  # This should never deviate

        cases.dataType.elementType.add("submitter_id", types.StringType())

        return schema

    def _build_from_scratch(self, input_dfs: PrimaryAliquotInputs) -> sql.DataFrame:
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
        query = {"query": {"bool": {"must": filters}}}

        return self._get_primary_aliquot_df(query).select(
            "file_id",
            "case_id",
            "case.submitter_id",
        )


class IndexBuilderInputs(TypedDict):
    gene_model_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class IndexBuilder(
    bases.IndexBuilder[gene_expression.IndexBuilder, IndexBuilderInputs]
):
    """
    A builder class for loading gene expression data.
    """

    __slots__ = ("_doc_dataframe_util",)

    def __init__(
        self,
        config: gene_expression.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        doc_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=IndexBuilderInputs,
            output=build.DataFrame.GENE_EXPRESSION,
        )

        self._doc_dataframe_util = doc_dataframe_util

    def _build_from_scratch(self, input_dfs: IndexBuilderInputs) -> sql.DataFrame:
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
            F.log2(F.col("uqfpkm") + 1).alias("log2_uqfpkm"),
        )

        return gene_expression_df.select(
            "case_id",
            "gene_expression_id",
            "gene_id",
            "log2_uqfpkm",
            "submitter_id",
            "symbol",
            "uqfpkm",
        )

    def _load_expression_values(
        self, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
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
