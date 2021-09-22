from exports import es_utils, indexd_utils
from typing import Iterable


import config
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql import types


RawAscatExpression = types.StructType(
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


class AscatBuilder:
    def __init__(
        self,
        document_dataframe_util: indexd_utils.IndexdDocumentDataFrameUtil,
        es_dataframe_util: es_utils.ElasticsearchDataFrameUtil,
        config: config.BaseConfig,
    ):
        self._document_dataframe_util = document_dataframe_util
        self._es_datafram_util = es_dataframe_util
        self._config = config

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = self._document_dataframe_util.get_dataframe(doc_ids)

        return document_df.select(
            f.col("did").alias("file_id"),
            "gene_id",
            f.col("gene_name").alias("symbol"),
            f.col("chromosome").alias("gene_chromosome"),
            f.col("start").alias("start_position"),
            f.col("end").alias("end_position"),
            "copy_number",
        )

    def _build_file_df(self, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
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

        if self._config.projects:
            body["query"]["bool"]["must"].append(
                {"terms": {"cases.project.project_id": self._config.projects}}
            )

        file_df = (
            self._es_datafram_util.get_dataframe(
                es_utils.Index.File, query=body, include_fields=included_fields
            )
            .select(
                "file_id",
                f.explode("cases").alias("case"),
            )
            .select(
                "file_id",
                f.col("case.case_id").alias("case_id"),
                f.explode("case.samples").alias("sample"),
            )
            .select(
                "file_id",
                "case_id",
                f.explode("sample.portions").alias("portion"),
            )
            .select(
                "file_id",
                "case_id",
                f.explode("portion.analytes").alias("analyte"),
            )
            .select(
                "file_id",
                "case_id",
                f.explode("analyte.aliquots").alias("aliquot"),
            )
            .select(
                "file_id",
                "case_id",
                f.col("aliquot.aliquot_id").alias("aliquot_id"),
            )
        )

        return file_df.join(primary_aliquot_df, on=["aliquot_id"]).select(
            "file_id", "case_id", "aliquot_id"
        )

    def build(self, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
        primary_aliquot_df = primary_aliquot_df.select("aliquot_id")
        file_df = self._build_file_df(primary_aliquot_df)
        doc_ids = file_df.select("file_id").distinct().collect()
        document_df = self._build_document_df(doc_ids)
        ascat_df = document_df.join(file_df, on=["file_id"]).join(
            primary_aliquot_df, on=["aliquot_id"]
        )

        print(document_df.select("file_id").distinct().collect())
        print(file_df.select("file_id").distinct().collect())

        return ascat_df.select(
            "file_id",
            "case_id",
            "aliquot_id",
            "gene_id",
            "symbol",
            "gene_chromosome",
            "start_position",
            "end_position",
            "copy_number",
        )
