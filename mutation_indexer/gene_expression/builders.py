from collections.abc import Iterable, Sequence
from typing import List, TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import es_utils, indexd_utils, schemas
from mutation_indexer.builders import bases
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import configuration


def _get_primary_aliquot_filters(projects: Sequence[str]) -> List[dict]:
    filters: List[dict] = [
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
    bases.PrimaryAliquotBuilder[configuration.Builder, PrimaryAliquotInputs]
):
    def __init__(
        self,
        config: configuration.Builder,
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
            input_type=PrimaryAliquotInputs,
            output=build.DataFrame.PRIMARY_ALIQUOT,
        )

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
        filters = _get_primary_aliquot_filters(self._config.projects)
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


class ExpressionValueInputs(TypedDict):
    gene_model_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class ExpressionValueBuilder(
    bases.InputBuilder[configuration.Builder, ExpressionValueInputs]
):
    """
    An input builder class for loading gene expression values.
    """

    __slots__ = ("_doc_dataframe_util",)

    def __init__(
        self,
        config: configuration.Builder,
        spark_session: sql.SparkSession,
        doc_dataframe_util: indexd_utils.DataFrameUtil,
    ):
        super().__init__(
            config,
            spark_session,
            input_type=ExpressionValueInputs,
            output=build.DataFrame.EXPRESSION_VALUE,
        )

        self._doc_dataframe_util = doc_dataframe_util

    def _build_from_scratch(self, input_dfs: ExpressionValueInputs) -> sql.DataFrame:
        """
        Creates a data frame containing the gene expression values contained within
        each file of the ge primary aliquot data. This excludes any expression values
        associated with any non-protein coding genes.

        Args:
            gene_model_df: The output of the GeneModelBuilder.
            gene_expression_primary_aliquot_df: the output of the GeneExpressionPrimaryAliquotBuilder

        Returns:
            a data frame of expression values associated with the file containing them

            gene_expression_value {}
            |---file_id
            +---genes [{}]
                |---expression_value
                |---gene_id
                +---symbol
        """
        gene_model_df = input_dfs["gene_model_df"]
        primary_aliquot_df = input_dfs["primary_aliquot_df"]

        pc_genes_df = gene_model_df.filter(
            F.col("biotype") == F.lit("protein_coding")
        ).select(F.col("_gene_id").alias("gene_id"), "symbol")

        ge_values_df = self.load_gene_expression_files(primary_aliquot_df)

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
        Load the gene expression data from the files referenced in the primary aliquot df

        Args:
            primary_aliquot_df: The dataframe of primary aliquot data for all
                "STAR - Counts files"
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
            F.element_at(F.split("gene_id", "\\."), 1).alias("gene_id"),
            F.col("fpkm_uq_unstranded").alias("expression_value"),
            F.col("did").alias("file_id"),
        )


class CaseInputs(TypedDict):
    primary_aliquot_df: sql.DataFrame


class CaseBuilder(bases.InputBuilder[configuration.Builder, CaseInputs]):
    """
    An input builder class for loading case data related to gene expression.
    """

    def __init__(
        self, config: configuration.Builder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(
            config, spark_session, input_type=CaseInputs, output=build.DataFrame.CASE
        )

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        """
        Creates a data frame containing the case data associated with the aliquots in the ge
        primary aliquot data and their related file_id.

        Args:
            input_dfs: contains the output of the primary aliquot data for the build.

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
        initial_df = input_dfs["primary_aliquot_df"]

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


class GeneExpressionInputs(TypedDict):
    case_df: sql.DataFrame
    expression_value_df: sql.DataFrame


class GeneExpressionBuilder(
    bases.IndexBuilder[configuration.IndexBuilder, GeneExpressionInputs]
):
    """
    A builder class for loading gene expression data.
    """

    def __init__(
        self,
        config: configuration.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=GeneExpressionInputs,
            output=build.DataFrame.GENE_EXPRESSION,
        )

    def _build_from_scratch(self, input_dfs: GeneExpressionInputs) -> sql.DataFrame:
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
        case_df = input_dfs["case_df"]
        expression_value_df = input_dfs["expression_value_df"]

        return case_df.join(expression_value_df, on="file_id", how="inner").select(
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
