import logging
from collections.abc import Iterable

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import TypedDict

from mutation_indexer import indexd_utils, schemas
from mutation_indexer.builders import bases, utils
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build

UUIDS_STRUCT = schemas.load_schema("builders/ascat/uuids.yaml")

logger = logging.getLogger(__name__)


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
) -> dict[str, str]:
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
    """
    Adds the cnv_change value to the data frame. This is calculated based on the
    modal values in each file. Any value less then the smallest modal value is a
    Loss while any value greater than the maximum mode is considered a Gain. All
    other values are neutral and are dropped from the data.

    METHOD:
    This is calculated by grouping all copy_numbers in a file and getting a count
    of their occurances/frequency. Then the counts are grouped again by file; in
    this aggregation, the min and max copy number are taken as the upper and
    lower ploity for a given count/frequency.

    Then the maximum count/frequency is calculated from aggregating the original
    counts based on file id and taking the max count. This data frame now has the
    count of the modal value(s).

    Using the above two data frames the modal count is then inner joined into the
    ploity data frame to give us the ploity values for a given file. This is then
    joined into the original data frame by file id to give every row a
    upper_ploity_number and lower_ploity_number which is used to select the
    cnv_change column in the returned data frame.

    Args:
        document_df: the data frame of ascat document data

    Returns:
        the bare info needed from the ascat document including cnv_change

        data {}
        |---cnv_change
        |---file_id
        +---gene_id
    """
    ploidy_df = document_df.groupBy("file_id", "copy_number").agg(
        F.count("*").alias("count")
    )
    ploidy_window = sql.Window().partitionBy("file_id", "count")
    mode_window = sql.Window().partitionBy("file_id").orderBy(F.col("count").desc())
    ploidy_df = ploidy_df.select(
        "file_id",
        F.min("copy_number").over(ploidy_window).alias("lower_ploidy_number"),
        F.max("copy_number").over(ploidy_window).alias("upper_ploidy_number"),
        F.row_number().over(mode_window).alias("row_number"),
    ).where(F.col("row_number") == 1)
    document_df = document_df.join(ploidy_df, on="file_id")
    cnv_change = (
        F.when(F.col("copy_number") > F.col("upper_ploidy_number"), "Gain")
        .when(F.col("copy_number") < F.col("lower_ploidy_number"), "Loss")
        .otherwise(None)
        .alias("cnv_change")
    )

    return document_df.select(
        cnv_change,
        "file_id",
        "gene_id",
    ).na.drop(subset="cnv_change")


class ASCATInputs(TypedDict):
    ascat_metadata_df: sql.DataFrame
    gene_model_df: sql.DataFrame


class ASCATBuilder(bases.InputBuilder[viz.ASCATBuilder, ASCATInputs]):
    __slots__ = ("_document_dataframe_util",)

    def __init__(
        self,
        config: viz.ASCATBuilder,
        spark_session: sql.SparkSession,
        document_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=ASCATInputs, output=build.DataFrame.ASCAT
        )

        self._document_dataframe_util = document_dataframe_util

    async def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = await self._document_dataframe_util.get_dataframe(
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

    async def _build_from_scratch(self, input_dfs: ASCATInputs) -> sql.DataFrame:
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
        if self._config.omit_cnv_data:
            return load_empty_ascat_data(self._spark_session)

        ascat_metadata_df = input_dfs["ascat_metadata_df"]
        gene_model_df = input_dfs["gene_model_df"]

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
            .where(utils.is_protein_coding())
            .where(utils.is_between_chr1_and_chr22())
        )
        document_df = await self._build_document_df(
            r.file_id for r in ascat_metadata_df.select("file_id").toLocalIterator()
        )
        ascat_df = document_df.join(ascat_metadata_df, on=["file_id"]).join(
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


def load_empty_ascat_data(spark_session: sql.SparkSession) -> sql.DataFrame:
    """
    Creates and empty dataframe with no data for omitting all cnv data from the
    output indices.

    TODO: DEV-1000: Remove this omission process from the code.
    """
    schema = schemas.load_schema("builders/ascat/final_ascat.json")

    return spark_session.createDataFrame((), schema=schema)
