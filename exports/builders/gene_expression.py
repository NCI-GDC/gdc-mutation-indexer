from functools import partial

from pyspark.sql.functions import col, collect_list, explode, lit, struct, udf
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from exports.builders.base_builder import BaseBuilder
from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.utils import get_gene_expression_metadata

GeneExpression = StructType([
    StructField("gene_id", StringType()),
    StructField("expression_value", FloatType()),
])


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

        case_ge_df = case_df.join(ge_df, "file_url")

        gene_expressions = udf(parse_gene_expressions, ArrayType(GeneExpression))

        final_df = case_ge_df.\
            withColumn("genes", gene_expressions(col("file_content"))).\
            drop("file_url").\
            drop("file_content")

        return final_df

    def _load_gene_expression_files(self, file_urls, batch_size=500):
        self.logger.info("Loading gene expression files")

        file_batches = []
        batch_n = 0

        # make batches
        while batch_n * batch_size < len(file_urls):
            file_batches.append(file_urls[batch_n * batch_size:(batch_n + 1) * batch_size])
            batch_n += 1

        ge_df = None
        # load gene expression file contents into a single row for further processing
        for i, file_batch in enumerate(file_batches):
            self.logger.info("Loading batch {}/{}".format(i+1, len(file_batches)))

            # Is it too hacky to access a "private" property here? Should we just
            # make this DF outside of this builder, where the Spark context is
            # available?
            rdd = self.sqlContext._sc.wholeTextFiles(",".join(file_batch))
            df_from_rdd = self.sqlContext.createDataFrame(
                rdd,
                schema=StructType([
                    StructField("file_url", StringType()),
                    StructField("file_content", StringType()),
                ])
            )

            if ge_df is None:
                ge_df = df_from_rdd
                continue

            ge_df = ge_df.union(df_from_rdd)

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
