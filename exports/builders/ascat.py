from typing import Iterable

import config
from exports import es_utils, indexd_utils
from exports.builders import base_input_builder
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

RawAscatStruct = types.StructType(
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
        self._es_dataframe_util = es_dataframe_util

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = self._document_dataframe_util.get_dataframe(
            doc_ids, RawAscatStruct
        )

        return document_df.select(
            F.col("did").alias("file_id"),
            "gene_id",
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
            self._es_dataframe_util.get_dataframe(
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

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        primary_aliquot_df = kwargs["primary_aliquot_df"]
        primary_aliquot_df = primary_aliquot_df.where(
            F.col("entity") == F.lit("file")
        ).select("file_id", "aliquot_id")

        file_df = (
            self._build_file_df()
            .join(primary_aliquot_df, on=["file_id", "aliquot_id"])
            .select("file_id", "case_id", "aliquot_id")
        )

        doc_ids = (
            row.file_id for row in file_df.select("file_id").distinct().collect()
        )
        document_df = self._build_document_df(doc_ids)

        return document_df.join(file_df, on=["file_id"]).select(
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
