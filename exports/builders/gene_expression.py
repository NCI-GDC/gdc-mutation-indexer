from pyspark.sql.functions import collect_list, explode, input_file_name, struct, udf
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from exports.builders import genes
from exports.builders.base_builder import BaseBuilder
from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.utils import get_gene_expression_metadata

GeneExpression = StructType([
    StructField("gene_id", StringType()),
    StructField("expression_value", DoubleType()),
])


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

        case_ge_df = case_df.join(ge_df, "file_url").drop("file_url")

        return case_ge_df

    def _load_gene_expression_files(self, file_urls, batch_size=500):
        self.logger.info("Loading gene expression files")

        # Batch the URLs so we don't pass 10000+ URLs to Spark all at once.
        # TODO: Is Spark actually cool with getting 10000+ URLs all at once?
        file_batches = [
            file_urls[offset : offset + batch_size]
            for offset in range(0, len(file_urls), batch_size)
        ]
        # Load gene expression file contents, one row per line.
        # Include the file URL on each line so we can join with the case DF later.
        ge_df = None
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
            batch_df = batch_df.filter(self._is_gene_included("gene_id"))
            batch_df = batch_df.withColumn("file_url", input_file_name())

            if ge_df is None:
                ge_df = batch_df
            else:
                ge_df = ge_df.union(batch_df)

        ge_df = ge_df.select(
            "file_url", struct(GeneExpression.fieldNames()).alias("gene")
        )

        return ge_df

    # TODO See if there's any performance difference if we make this return a function
    # so it doesn't have to examine self.
    @udf(returnType=BooleanType)
    def _is_gene_included(self, gene_id):
        return (
            not self.config.include_protein_coding_genes_only
            or genes.is_protein_coding(gene_id)
        )


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
