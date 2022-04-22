from typing import Any, Dict, Iterable

import elasticsearch
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

import config
from exports import es_utils, indexd_utils, schemas
from exports.builders import base_input_builder, utils

UUIDS_STRUCT = schemas.load_schema("builders/ascat/uuids.yaml")


def _strip_gene_id() -> sql.Column:
    gene_id = F.col("gene_id")

    return F.element_at(F.split(gene_id, r"\."), 1)


@F.udf(returnType=UUIDS_STRUCT)
def _generate_uuids(
    chromosome: str,
    start_position: int,
    end_position: int,
    cnv_change: int,
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
    cnv_id = utils.generate_uuid5(chromosome, start_position, end_position, cnv_change)

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
        "cnv_change",
        "symbol",
        "gene_id",
        "is_cancer_gene_census",
        "biotype",
        "case_id",
        "aliquot_id",
    )

    ascat_df = ascat_df.withColumn(
        "uuids", uuids
    )  # This adds the uuids struct used below.

    return ascat_df.select("*", "uuids.*")


def _add_cnv_change(document_df: sql.DataFrame) -> sql.DataFrame:
    ploidy_df = document_df.groupby("file_id", "copy_number").count()
    ploidy_df = (
        ploidy_df.groupBy("file_id", "count")
        .agg(F.min("copy_number"), F.max("copy_number"))
        .select(
            "count",
            "file_id",
            F.col("max(copy_number)").alias("upper_ploity_number"),
            F.col("min(copy_number)").alias("lower_ploity_number"),
        )
    )
    max_count_df = (
        ploidy_df.groupBy("file_id")
        .max("count")
        .withColumnRenamed("max(count)", "count")
    )
    ploidy_df = ploidy_df.join(max_count_df, on=["file_id", "count"]).select(
        "file_id", "upper_ploity_number", "lower_ploity_number"
    )
    document_df = document_df.select("file_id", "gene_id", "copy_number").join(
        ploidy_df, on="file_id"
    )
    cnv_change = (
        F.when(F.col("copy_number") > F.col("upper_ploity_number"), "Gain")
        .when(F.col("copy_number") < F.col("lower_ploity_number"), "Loss")
        .otherwise(None)
        .alias("cnv_change")
    )

    return document_df.select(
        "file_id",
        "gene_id",
        cnv_change,
    ).na.drop(subset=["cnv_change"])


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
            doc_ids, schema=schemas.load_schema("builders/ascat/ascat_document.yaml")
        )

        document_df = (
            document_df.withColumn(
                "chromosome",
                F.coalesce(
                    F.regexp_replace("chromosome", "chr", "").cast(types.IntegerType()),
                    F.lit(-1),
                ),
            )
            .where(F.col("chromosome").between(1, 22))
            .select(
                "copy_number",
                F.col("did").alias("file_id"),
                _strip_gene_id().alias("gene_id"),
            )
        )

        return _add_cnv_change(document_df)

    def _build_file_df(self, file_ids: Iterable[str]) -> sql.DataFrame:
        body = {"query": {"terms": {"file_id": list(file_ids)}}}

        return (
            self._es_dataframe_util.get_dataframe(es_utils.Index.File, query=body)
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
                        {"term": {"data_type": "Gene Level Copy Number"}},
                    ],
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "experimental_strategy": "Genotyping Array"
                                        }
                                    },
                                    {"term": {"analysis.workflow_type": "ASCAT2"}},
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"experimental_strategy": "WGS"}},
                                    {"term": {"analysis.workflow_type": "AscatNGS"}},
                                ]
                            }
                        },
                    ],
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
        self,
        primary_aliquot_df: sql.DataFrame,
        gene_model_df: sql.DataFrame,
        **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """Builds the ASCAT dataframe

        ascat {}
        |---_id
        |---aliquot_id
        |---available_variation_data
        |---biotype
        |---canonical_transcript_id
        |---canonical_transcript_length
        |---canonical_transcript_length_cds
        |---canonical_transcript_length_genomic
        |---case_id
        |---chromosome
        |---cnv_change
        |---cnv_id
        |---consequence_id
        |---cytoband
        |---description
        |---end_position
        |---entrez_gene
        |---gene_chromosome
        |---gene_end
        |---gene_id
        |---gene_level_cn
        |---gene_start
        |---gene_strand
        |---hgnc
        |---is_cancer_gene_census
        |---name
        |---ncbi_build
        |---observation_id
        |---occurrence_id
        |---omim_gene
        |---start_position
        |---symbol
        |---synonyms
        |---transcripts [{}]
        |   +---(see gene_model.py)
        |---uniprotkb_swissprot
        |---variant_caller
        +---variant_status
        """
        dids = self._get_document_ids()
        primary_aliquot_df = primary_aliquot_df.where(
            F.col("entity") == F.lit("file")
        ).select("file_id", "aliquot_id")
        gene_model_df = (
            gene_model_df.select(
                F.col("_gene_id").alias("gene_id"),
                "_id",
                "biotype",
                "canonical_transcript_id",
                "chromosome",
                "cytoband",
                "description",
                F.col("gene_end").alias("end_position"),
                "entrez_gene",
                F.col("chromosome").alias("gene_chromosome"),
                "gene_end",
                "gene_start",
                "gene_strand",
                "hgnc",
                "is_cancer_gene_census",
                "name",
                "omim_gene",
                F.col("gene_start").alias("start_position"),
                "synonyms",
                "symbol",
                "transcripts",
                "uniprotkb_swissprot",
            )
            .where(F.col("biotype") == F.lit("protein_coding"))
            .where(
                F.coalesce(
                    F.col("chromosome").cast(types.IntegerType()), F.lit(-1)
                ).between(0, 22)
            )
        )
        file_df = self._build_file_df(dids)
        file_df = file_df.join(primary_aliquot_df, on=["file_id", "aliquot_id"]).select(
            "file_id", "case_id", "aliquot_id"
        )
        document_df = self._build_document_df(dids)
        ascat_df = document_df.join(file_df, on=["file_id"]).join(
            gene_model_df, on=["gene_id"]
        )
        ascat_df = utils.add_canonical_transcript_lengths(ascat_df)
        ascat_df = _add_uuids(ascat_df)

        return ascat_df.select(
            "_id",
            "aliquot_id",
            F.lit("cnv").alias("available_variation_data"),
            "biotype",
            "canonical_transcript_id",
            "canonical_transcript_length",
            "canonical_transcript_length_cds",
            "canonical_transcript_length_genomic",
            "case_id",
            "chromosome",
            "cnv_change",
            "cnv_id",
            "consequence_id",
            "cytoband",
            "description",
            "end_position",
            "entrez_gene",
            "gene_chromosome",
            "gene_end",
            "gene_id",
            F.lit(True).alias("gene_level_cn"),
            "gene_start",
            "gene_strand",
            "hgnc",
            "is_cancer_gene_census",
            "name",
            F.lit("GRCh38").alias("ncbi_build"),
            "observation_id",
            "occurrence_id",
            "omim_gene",
            "start_position",
            "symbol",
            "synonyms",
            "transcripts",
            "uniprotkb_swissprot",
            F.lit("ASCAT").alias("variant_caller"),
            F.lit("Tumor Only").alias("variant_status"),
        )


def load_empty_ascat_data(sql_context: sql.SQLContext) -> sql.DataFrame:
    """
    Creates and empty dataframe with no data for omitting all cnv data from the
    output indices.

    TODO: DEV-1000: Remove this omission process from the code.
    """
    schema = schemas.load_schema("builders/ascat/final_ascat.json")

    return sql_context.createDataFrame((), schema=schema)
