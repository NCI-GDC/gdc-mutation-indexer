from exports.builders.base_builder import BaseBuilder
from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.gene_model import GeneModelBuilder
from exports.builders.primary_aliquot import PrimaryAliquotBuilder
from pyspark import sql
from pyspark.sql.functions import (
    col,
    collect_list,
    explode,
    input_file_name,
    struct,
    udf
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CASE_METADATA = [
    "case_id",
    "demographic.days_to_death",
    "demographic.ethnicity",
    "demographic.gender",
    "demographic.race",
    "demographic.vital_status",
    "submitter_id",
    "project.project_id",
]

CASE_NESTED_METADATA = [
    "diagnoses.age_at_diagnosis",
    "samples.sample_type"
]


RawGeneExpression = StructType([
    StructField("raw_gene_id", StringType()),
    StructField("expression_value", DoubleType()),
])

GENE_EXPRESSION_SCHEMA = {
    'type': 'struct',
    'fields': [
        {'metadata': {}, 'nullable': True, 'type': 'string', 'name': 'case_id'},
        {'metadata': {}, 'nullable': True,
         'type': {'valueContainsNull': True, 'valueType': 'string', 'type': 'map',
                  'keyType': 'string'}, 'name': 'demographic'},
        {'metadata': {}, 'nullable': True, 'type': {
            'elementType': {'valueContainsNull': True, 'valueType': 'long', 'type': 'map',
                            'keyType': 'string'}, 'containsNull': True, 'type': 'array'},
         'name': 'diagnoses'},
        {'metadata': {}, 'nullable': True, 'type': 'string', 'name': 'file_id'},
        {'metadata': {}, 'nullable': True, 'type': 'string', 'name': 'file_url'},
        {'metadata': {}, 'nullable': True,
         'type': {'valueContainsNull': True, 'valueType': 'string', 'type': 'map',
                  'keyType': 'string'}, 'name': 'project'},
        {'metadata': {}, 'nullable': True, 'type': {
            'elementType': {'valueContainsNull': True, 'valueType': 'string',
                            'type': 'map', 'keyType': 'string'}, 'containsNull': True,
            'type': 'array'}, 'name': 'samples'},
        {'metadata': {}, 'nullable': True, 'type': 'string', 'name': 'submitter_id'}
    ]
}


@udf(returnType=StringType())
def trim_gene_id(raw_gene_id):
    return raw_gene_id.split(".")[0]


class GeneExpressionInputBuilder:
    supported_workflow_types = ["HTSeq - FPKM-UQ"]

    def __init__(self, config, spark_session: sql.SparkSession, *args, **kwargs):
        super().__init__(config, spark_session, *args, **kwargs)  # type: ignore

        self._spark_session = spark_session
        self.config = config
        self._primary_aliquot_data = None
        self.primary_aliquot_builder = PrimaryAliquotBuilder(config, spark_session)

    def get_primary_aliquot_data(self):
        """
        Gets the primary aliquot data originating from ES from memory or loads it into memory if
        not there. This data includes a data frame with each file containing the primary aliquot 
        for a case and the associated case data. The primary aliquot data also has a list of the 
        file urls containing the primary aliquots.

        Returns:
            GeneExpressionPrimaryAliquotData: the primary aliquot data for the project
        """
        if self._primary_aliquot_data is not None:
            return self._primary_aliquot_data

        self._primary_aliquot_data = (
            self.primary_aliquot_builder.build_gene_expression_primary_aliquot_data(
                self.supported_workflow_types,
            )
        )

        return self._primary_aliquot_data


class GeneExpressionValueInputBuilder(GeneExpressionInputBuilder, BaseInputBuilder):

    def build_from_scratch(self):
        gm_df = GeneModelBuilder(self.config, self._spark_session).build()

        pc_genes_df = gm_df.filter(gm_df.biotype == "protein_coding").select("_gene_id", "symbol")

        ge_values_df = self.load_gene_expression_files_into_df()

        ge_values_df = ge_values_df.\
            join(pc_genes_df, ge_values_df.gene_id == pc_genes_df._gene_id).\
            drop("_gene_id").\
            withColumn("genes", struct("gene_id", "expression_value", "symbol")).\
            drop("gene_id", "expression_value", "symbol").\
            groupBy("file_url").\
            agg(collect_list("genes").alias("genes"))

        return ge_values_df

    def get_urls(self):
        return self.get_primary_aliquot_data().file_urls

    def load_gene_expression_files_into_df(self, batch_size=500):
        self.logger.info("Loading gene expression files")

        file_urls = self.urls

        # make batches
        file_batches = [
            file_urls[offset:offset + batch_size]
            for offset in range(0, len(file_urls), batch_size)
        ]

        ge_df = None
        # Batch files and load them into DF
        for i, file_batch in enumerate(file_batches):
            self.logger.info("Loading batch {}/{}".format(i+1, len(file_batches)))

            batch_df = self._spark_session.read.csv(
                file_batch,
                schema=RawGeneExpression,
                sep="\t",
                header=False,
                enforceSchema=True,
                mode="FAILFAST",
            )
            batch_df = batch_df.withColumn("file_url", input_file_name())

            batch_df = batch_df.\
                withColumn("gene_id", trim_gene_id(col("raw_gene_id"))).\
                drop("raw_gene_id")

            if ge_df is None:
                ge_df = batch_df
                continue

            ge_df = ge_df.union(batch_df)

        return ge_df


class GeneExpressionCaseInputBuilder(GeneExpressionInputBuilder, BaseInputBuilder):

    def build_from_scratch(self):
        initial_df = self.get_primary_aliquot_data().primary_aliquot_df

        # NOTE: diagnoses is a nested document, so we are flattening it by
        #   simply aggregating age_at_diagnosis values into an array
        flat_diagnosis_df = initial_df.\
            select("case_id", explode("diagnoses").alias("diagnosis")).\
            select("case_id", "diagnosis.age_at_diagnosis").\
            groupby("case_id").\
            agg(collect_list("age_at_diagnosis").alias("age_at_diagnosis"))

        case_ge_df = initial_df.\
            select(*(CASE_METADATA + ["file_url"])).\
            join(flat_diagnosis_df, "case_id")

        return case_ge_df


class GeneExpressionBuilder(BaseBuilder):
    index_name = "gene_expression"
    # NOTE: We might need a synthetic ID here, when we add support for Aliquot
    #   level gene expressions
    id_field = "case_id"

    def __init__(self, *args, **kwargs):
        super(GeneExpressionBuilder, self).__init__(*args, **kwargs)

        self.gene_expression = None
        self.gene_expression_backup = "neither"

    def build(self, case_df, ge_values_df):
        """
        Combine two input data frames into a final gene_expression data frame

        Args:
            case_df: expected to be a gene_expression case DF
            ge_values_df: expected to be a gene_expression values DF
        """

        # NOTE: the default join strategy is 'inner', so any extra cases/expression
        #   values will be dropped, which is expected
        self.gene_expression = case_df.join(ge_values_df, "file_url").drop("file_url")

        return self
