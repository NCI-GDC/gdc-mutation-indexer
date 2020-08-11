from pyspark.sql.functions import (
    col,
    collect_list,
    explode,
    input_file_name,
    lit,
    struct,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from exports.builders.base_builder import BaseBuilder
from exports.builders.utils import get_gene_expression_metadata


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


GeneExpression = StructType([
    StructField("raw_gene_id", StringType()),
    StructField("expression_value", DoubleType()),
])


@udf(returnType=StringType())
def trim_gene_id(raw_gene_id):
    return raw_gene_id.split(".")[0]


class GeneExpressionBuilder(BaseBuilder):
    index_name = "gene_expression"
    id_field = "case_id"

    def __init__(self, *args, **kwargs):
        super(GeneExpressionBuilder, self).__init__(*args, **kwargs)

        self.gene_expression = None
        self.gene_expression_backup = "neither"

    def build(self):

        files_metadata = self._get_gene_expression_files_metadata()

        root_df = self.build_gene_expression_root_df(files_metadata)

        values_df = self.build_gene_expression_values_df(files_metadata)

        self.gene_expression = root_df.join(values_df, "file_url").drop("file_url")

        return self

    def _get_gene_expression_files_metadata(self):
        file_source = ["cases." + field for field in CASE_METADATA + CASE_NESTED_METADATA]

        ge_files_metadata = get_gene_expression_metadata(
            self.config,
            sample_types=["Primary Tumor", "Tumor"],
            source=file_source,
        )

        return ge_files_metadata

    def build_gene_expression_root_df(self, ge_metadata):
        initial_df = self.sqlContext.createDataFrame(ge_metadata)

        # NOTE: diagnoses is a nested document, so we are simply aggregating
        #   age_at_diagnosis values into an array
        flat_diagnosis_df = initial_df.\
            select("case_id", explode("diagnoses").alias("diagnosis")).\
            select("case_id", "diagnosis.age_at_diagnosis").\
            groupby("case_id").\
            agg(collect_list("age_at_diagnosis").alias("age_at_diagnosis"))

        case_ge_df = initial_df.\
            select(*(CASE_METADATA+["file_url"])).\
            join(flat_diagnosis_df, "case_id")

        return case_ge_df

    def build_gene_expression_values_df(self, ge_metadata):
        ge_df = self.load_gene_expression_files_into_df([meta["file_url"] for meta in ge_metadata])

        gene_model_df = self.sqlContext.read.json(self.config.gene_model_file)

        protein_coding = gene_model_df.\
            filter(gene_model_df.biotype == "protein_coding").\
            select("_gene_id", "symbol")

        ge_values_df = ge_df.\
            join(protein_coding, ge_df.gene_id == protein_coding._gene_id).\
            drop("_gene_id").\
            withColumn("genes", struct("gene_id", "expression_value", "symbol")).\
            drop("gene_id", "expression_value", "symbol").\
            groupBy("file_url").\
            agg(collect_list("genes").alias("genes"))

        return ge_values_df

    def load_gene_expression_files_into_df(self, file_urls, batch_size=500):
        self.logger.info("Loading gene expression files")

        # make batches
        file_batches = [
            file_urls[offset:offset + batch_size]
            for offset in range(0, len(file_urls), batch_size)
        ]

        ge_df = None
        # Batch files and load them into DF
        for i, file_batch in enumerate(file_batches):
            self.logger.info("Loading batch {}/{}".format(i+1, len(file_batches)))

            batch_df = self.sqlContext.read.csv(
                file_batch,
                schema=GeneExpression,
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
