import uuid
from typing import Any, Dict, Iterable

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

import config
from exports import es_utils, indexd_utils
from exports.builders import base_input_builder


RAW_ASCAT_STRUCT = types.StructType(
    [
        types.StructField("gene_id", types.StringType()),
        types.StructField("gene_name", types.StringType()),
        types.StructField("chromosome", types.StringType()),
        types.StructField("start", types.IntegerType()),
        types.StructField("end", types.IntegerType()),
        types.StructField("copy_number", types.IntegerType()),
        types.StructField("min_copy_number", types.IntegerType()),
        types.StructField("max_copy_number", types.IntegerType()),
    ]
)

UUIDS_STRUCT = types.StructType(
    [
        types.StructField("cnv_id", types.StringType()),
        types.StructField("consequence_id", types.StringType()),
        types.StructField("occurance_id", types.StringType()),
        types.StructField("observation_id", types.StringType()),
    ]
)


def _parse_gene_id(col_name) -> sql.Column:
    gene_id = F.col(col_name)

    return F.element_at(F.split(gene_id, r"\."), 1)


@F.udf(returnType=UUIDS_STRUCT)
def _generate_uuids(
    chromosome: str,
    start_position: int,
    end_position: int,
    copy_number: int,
    symbol: str,
    gene_id: str,
    is_cancer_gene_census: bool,
    biotype: str,
    case_id: str,
    aliquot_id: str,
) -> Dict[str, str]:
    def generate_uuid(*values: Any) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(v) for v in values)))

    cnv_id = generate_uuid(chromosome, start_position, end_position, copy_number)

    return {
        "cnv_id": cnv_id,
        "consequence_id": generate_uuid(
            symbol, gene_id, is_cancer_gene_census, biotype
        ),
        "occurance_id": generate_uuid(cnv_id, case_id),
        "observation_id": generate_uuid(cnv_id, case_id, aliquot_id),
    }


def _add_uuids(ascat_df: sql.DataFrame) -> sql.DataFrame:
    uuids = _generate_uuids(
        "gene_chromosome",
        "start_position",
        "end_position",
        "copy_number",
        "symbol",
        "gene_id",
        "is_cancer_gene_census",
        "biotype",
        "case_id",
        "aliquot_id",
    )

    ascat_df = ascat_df.withColumn("uuids", uuids)
    ascat_df = ascat_df.withColumn("cnv_id", F.col("uuids.cnv_id"))
    ascat_df = ascat_df.withColumn("consequence_id", F.col("uuids.consequence_id"))
    ascat_df = ascat_df.withColumn("occurance_id", F.col("uuids.occurance_id"))
    ascat_df = ascat_df.withColumn("observation_id", F.col("uuids.observation_id"))

    return ascat_df


class AscatBuilder(base_input_builder.BaseInputBuilder):
    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        document_dataframe_util: indexd_utils.DataFrameUtil,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        super().__init__(config, sqlContext, "ascat")

        self._document_dataframe_util = document_dataframe_util
        self._es_datafram_util = es_dataframe_util

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = self._document_dataframe_util.get_dataframe(
            doc_ids, RAW_ASCAT_STRUCT
        )

        return document_df.select(
            F.col("did").alias("file_id"),
            _parse_gene_id("gene_id").alias("gene_id"),
            F.col("gene_name").alias("symbol"),
            F.col("chromosome").alias("gene_chromosome"),
            F.col("start").alias("start_position"),
            F.col("end").alias("end_position"),
            "copy_number",
        )

    def _build_file_df(self) -> sql.DataFrame:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"experimental_strategy": "Genotyping Array"}},
                        {"match": {"data_type": "Gene Level Copy Number"}},
                        {"match": {"analysis.workflow_type": "ASCAT2"}},
                    ]
                }
            }
        }
        included_fields = [
            "file_id",
            "cases.case_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
        ]

        if self.config.projects:
            body["query"]["bool"]["must"].append(
                {"terms": {"cases.project.project_id": self.config.projects}}
            )

        return (
            self._es_datafram_util.get_dataframe(
                es_utils.Index.File, query=body, include_fields=included_fields
            )
            .select(
                "file_id",
                F.explode("cases").alias("case"),
            )
            .select(
                "file_id",
                F.col("case.case_id").alias("case_id"),
                F.explode("case.samples").alias("sample"),
            )
            .select(
                "file_id",
                "case_id",
                F.explode("sample.portions").alias("portion"),
            )
            .select(
                "file_id",
                "case_id",
                F.explode("portion.analytes").alias("analyte"),
            )
            .select(
                "file_id",
                "case_id",
                F.explode("analyte.aliquots").alias("aliquot"),
            )
            .select(
                "file_id",
                "case_id",
                "aliquot.aliquot_id",
            )
        )

    def build_from_scratch(
        self, primary_aliquot_df: sql.DataFrame, gene_model_df, **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """Builds the ASCAT dataframe

        ascat_df {}
        |---file_id
        |---case_id
        |---aliquot_id
        |---gene_id
        |---symbol
        |---gene_chromosome
        |---start_position
        |---end_position
        |---copy_number (TODO: Switch to cnv_change)
        |---cnv_id
        |---consequence_id
        |---occurance_id
        |---observation_id
        """
        primary_aliquot_df = primary_aliquot_df.where(
            F.col("entity") == F.lit("file")
        ).select("file_id", "aliquot_id")
        gene_model_df = gene_model_df.select(
            F.col("_gene_id").alias("gene_id"),
            "is_cancer_gene_census",
            "biotype",
        )

        file_df = (
            self._build_file_df()
            .join(primary_aliquot_df, on=["file_id", "aliquot_id"])
            .select("file_id", "case_id", "aliquot_id")
        )

        rows = file_df.select("file_id").distinct().collect()
        document_df = self._build_document_df(row.file_id for row in rows)
        ascat_df = document_df.join(file_df, on=["file_id"]).join(
            gene_model_df, on=["gene_id"]
        )

        return _add_uuids(ascat_df).select(
            "file_id",
            "case_id",
            "aliquot_id",
            "gene_id",
            "symbol",
            "gene_chromosome",
            "start_position",
            "end_position",
            "copy_number",
            "cnv_id",
            "consequence_id",
            "occurance_id",
            "observation_id",
        )
