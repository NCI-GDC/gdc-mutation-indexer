import json
from typing import Any, Dict, Iterable

import elasticsearch
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
import pkg_resources

import config
from exports import es_utils, indexd_utils
from exports.builders import base_input_builder, utils

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
        types.StructField("occurrence_id", types.StringType()),
        types.StructField("observation_id", types.StringType()),
    ]
)


def _strip_gene_id() -> sql.Column:
    gene_id = F.col("gene_id")

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
    """Creates a uuid struct the following uuids (based on):
            cnv_id (chromosome, start_position, end_position, copy_number)
            consequence_id (symbol, gene_id, is_cancer_gene_census, biotype)
            occurrence_id (cnv_id, case_id)
            observation_id (cnv_id, case_id, aliquot_id)

        Returns: UUIDS_STRUCT
    """
    cnv_id = utils.generate_uuid5(chromosome, start_position, end_position, copy_number)

    return {
        "cnv_id": cnv_id,
        "consequence_id": utils.generate_uuid5(
            symbol, gene_id, is_cancer_gene_census, biotype
        ),
        "occurrence_id": utils.generate_uuid5(cnv_id, case_id),
        "observation_id": utils.generate_uuid5(cnv_id, case_id, aliquot_id),
    }


def _add_uuids(ascat_df: sql.DataFrame) -> sql.DataFrame:
    """Adds the following uuids to the dataframe:
            cnv_id
            consequence_id
            occurrence_id
            observation_id
        Which are created using the following columns from the input ascat_df:
            gene_chromosome
            start_position
            end_position
            copy_number
            symbol
            gene_id
            is_cancer_gene_census
            biotype
            case_id
            aliquot_id

        Args:
            ascat_df: The ascat dataframe with the documented columns present.

        Returns:
            Ascat data frame with the uuuids added.
    """
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

    return ascat_df.select("*", "uuids.*")


class AscatBuilder(base_input_builder.BaseInputBuilder):
    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        document_dataframe_util: indexd_utils.DataFrameUtil,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        super().__init__(config, sqlContext, "ascat")

        self._document_dataframe_util = document_dataframe_util
        self._es_dataframe_util = es_dataframe_util
        self._es_client = es_client

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = self._document_dataframe_util.get_dataframe(
            doc_ids, RAW_ASCAT_STRUCT
        )

        return document_df.select(
            F.col("did").alias("file_id"),
            _strip_gene_id().alias("gene_id"),
            F.col("gene_name").alias("symbol"),
            F.col("chromosome").alias("gene_chromosome"),
            F.col("start").alias("start_position"),
            F.col("end").alias("end_position"),
            "copy_number",
        )

    def _build_file_df(self, dids: Iterable[str]) -> sql.DataFrame:
        body = {"query": {"terms": {"file_id": list(dids)}}}
        included_fields = [
            "file_id",
            "cases.case_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
        ]

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

    def _get_document_ids(self) -> Iterable[str]:
        body: Dict[str, Any] = {
            "_source": ["file_id"],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"experimental_strategy": "Genotyping Array"}},
                        {"match": {"data_type": "Gene Level Copy Number"}},
                        {"match": {"analysis.workflow_type": "ASCAT2"}},
                    ]
                }
            },
        }

        if self.config.projects:
            body["query"]["bool"]["must"].append(
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {
                                "cases.project.project_id": list(
                                    project
                                    for project in self.config.projects
                                    if project.startswith("TCGA")
                                )
                            }
                        },
                    }
                }
            )
        else:
            body["query"]["bool"]["must"].append(
                {
                    "nested": {
                        "path": "cases",
                        "query": {"term": {"cases.project.program.name": "TCGA"}},
                    }
                }
            ) 

        hits = es_utils.iterate_es_results(
            self._es_client,
            index_name=self.config.graph_file_index,
            doc_type=self.config.graph_file_doc_type,
            query=body,
        )

        return tuple(hit["_source"]["file_id"] for hit in hits)

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
        dids = self._get_document_ids()
        primary_aliquot_df = primary_aliquot_df.where(
            F.col("entity") == F.lit("file")
        ).select("file_id", "aliquot_id")
        gene_model_df = gene_model_df.select(
            F.col("_gene_id").alias("gene_id"),
            "is_cancer_gene_census",
            "biotype",
        )

        file_df = (
            self._build_file_df(dids)
            .join(primary_aliquot_df, on=["file_id", "aliquot_id"])
            .select("file_id", "case_id", "aliquot_id")
        )

        document_df = self._build_document_df(dids)
        ascat_df = document_df.join(file_df, on=["file_id"]).join(
            gene_model_df, on=["gene_id"]
        )

        return _add_uuids(ascat_df).select(
            "aliquot_id",
            "biotype",
            "case_id",
            "consequence_id",
            "cnv_id",
            "end_position",
            "gene_chromosome",
            "gene_id",
            "observation_id",
            "occurrence_id",
            "start_position",
            "symbol",
        )


def _load_ascat_schema() -> types.StructType:
    schema_path = pkg_resources.resource_filename("exports.schemas", "builders/ascat/final_ascat.json")

    with open(schema_path, "r") as f:
        return types.StructType.fromJson(json.load(f))


def load_empty_ascat_data(sql_context: sql.SQLContext) -> sql.DataFrame:
    """
    Creates and empty dataframe with no data for omitting all cnv data from the
    output indices.

    TODO: DEV-1000: Remove this omission process from the code.
    """
    schema = _load_ascat_schema()

    return sql_context.createDataFrame((), schema=schema)
