from functools import partial

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
from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.utils import get_gene_expression_metadata

GeneExpression = StructType([
    StructField("raw_gene_id", StringType()),
    StructField("expression_value", DoubleType()),
])


@udf(returnType=StringType())
def trim_gene_id(raw_gene_id):
    return raw_gene_id.split(".")[0]


def parse_gene_expressions(file_content):
    stripped = file_content.strip()

    def make_row(gene_id, raw_value):
        return gene_id, float(raw_value)

    return [make_row(*row.split("\t")) for row in stripped.split("\n")]


class ExpressionCountsBuilder(BaseInputBuilder):
    def build_from_scratch(self):
        case_fields = [
            "case_id",
            "demographic.days_to_death",
            "demographic.ethnicity",
            "demographic.gender",
            "demographic.race",
            "demographic.vital_status",
            "submitter_id",
            "project.project_id",
        ]
        case_nested_fields = [
            "diagnoses.age_at_diagnosis",
            "samples.sample_type"
        ]

        file_source = ["cases." + field for field in case_fields + case_nested_fields]

        ge_metadata = get_gene_expression_metadata(
            self.config,
            sample_types=["Primary Tumor", "Tumor"],
            source=file_source,
        )

        initial_df = self.sqlContext.createDataFrame(ge_metadata)

        # FIXME: should we handle this in `get_gene_expression_metadata` instead?
        flat_diagnosis_df = initial_df.\
            select("case_id", explode("diagnoses").alias("diagnosis")).\
            select("case_id", "diagnosis.age_at_diagnosis").\
            groupby("case_id").\
            agg(collect_list("age_at_diagnosis").alias("age_at_diagnosis"))

        case_df = initial_df.select(*(case_fields+["file_url"])).join(flat_diagnosis_df, "case_id")

        ge_df = self._load_gene_expression_files([meta["file_url"] for meta in ge_metadata])

        ge_df = ge_df.\
            withColumn("genes", struct("gene_id", "expression_value")).\
            drop("gene_id", "expression_value").\
            groupby("file_url").\
            agg(collect_list("genes").alias("genes"))

        final_df = case_df.join(ge_df, "file_url").drop("file_url")

        return final_df

    def _load_gene_expression_files(self, file_urls, batch_size=500):
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


class GeneExpressionBuilder(BaseBuilder):
    index_name = "gene_expression"
    id_field = "case_id"

    def __init__(self, *args, **kwargs):
        super(GeneExpressionBuilder, self).__init__(*args, **kwargs)

        self.gene_expression = None
        self.gene_expression_backup = "neither"

    def build(self, ge_df):
        self.gene_expression = ge_df
        return self
