from pyspark.sql.functions import collect_list, explode, lit, struct
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from exports.builders.base_builder import BaseBuilder
from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.utils import get_gene_expression_metadata


def get_indexd_url(indexd_client, file_id):
    doc = indexd_client.get(file_id)

    if not doc:
        return None

    primary_types = ["cleversafe"]

    for url, meta in doc.urls_metadata.items():
        if meta.get("type") in primary_types and meta.get("state") == "validated":
            url = url.replace("s3://", "s3a://")
            return url

    return None


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

        file_source = ["cases." + col for col in case_fields + case_nested_fields]

        ge_metadata = get_gene_expression_metadata(
            self.config,
            sample_types=["Primary Tumor", "Tumor"],
            source=file_source,
        )

        ge_counts_df = None

        # FIXME: create RDD via `parallelize` and do map/union instead?
        for meta in ge_metadata:
            new_df = self._load_gene_expression_df(meta["case_id"], meta["file_id"])

            if not new_df:
                continue

            if ge_counts_df is None:
                ge_counts_df = new_df
                continue

            ge_counts_df = ge_counts_df.union(new_df)

        case_ge_df = self.sqlContext.createDataFrame(ge_metadata)

        array_dfs = None
        # TODO: For now we just aggregate the nested fields into an array.
        #   Should we change the gene_expression ES mappings and make these values
        #   nested and potentially add more fields to them?
        for nested, alias, field in [
            ("diagnoses", "diagnosis", "age_at_diagnosis"),
        ]:
            nested_df = case_ge_df.\
                select("case_id", explode(nested).alias(alias)).\
                select("case_id", "{}.{}".format(alias, field)).\
                groupby("case_id").\
                agg(collect_list(field).alias(field))

            if not array_dfs:
                array_dfs = nested_df
                continue

            array_dfs = array_dfs.join(nested_df, "case_id")

        case_df = case_ge_df.select(*case_fields).join(array_dfs, "case_id")

        ge_df = ge_counts_df.\
            groupby("case_id").\
            agg(collect_list("expression_data").alias("genes"))

        final_df = case_df.join(ge_df, "case_id")

        return final_df

    def _load_gene_expression_df(self, case_id, file_id):
        url = get_indexd_url(self.config.indexd, file_id)

        if url is None:
            self.logger.warning("No url found for file: {}".format(file_id))
            return None

        ge_schema = StructType(
            [
                StructField("gene_id", StringType(), nullable=False),
                StructField("expression_value", FloatType(), nullable=False),
            ]
        )

        ge_df = self.file_to_df(url, header=False, schema=ge_schema)

        # make an explicit struct, rather than columns
        ge_df = ge_df.select(struct("gene_id", "expression_value").alias("expression_data"))
        # add corresponding case_id
        ge_df = ge_df.withColumn("case_id", lit(case_id))

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
