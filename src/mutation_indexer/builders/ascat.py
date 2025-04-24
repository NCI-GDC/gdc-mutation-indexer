import logging
from collections.abc import Iterable

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import TypedDict

from mutation_indexer import indexd_utils, schemas
from mutation_indexer.builders import bases, utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

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
    cnv_change_5_category: int,
    symbol: str,
    gene_id: str,
    is_cancer_gene_census: bool,
    biotype: str,
    case_id: str,
    aliquot_id: str,
) -> dict[str, str]:
    """Creates a uuid struct the following uuids (based on):
        cnv_id (chromosome, start_position, end_position, cnv_change_5_category)
        consequence_id (symbol, gene_id, is_cancer_gene_census, biotype)
        occurrence_id (cnv_id, case_id)
        observation_id (cnv_id, case_id, aliquot_id)

    Returns: UUIDS_STRUCT
    """
    cnv_id = utils.generate_uuid5(
        chromosome, start_position, end_position, cnv_change_5_category
    )

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
        cnv_change_5_category
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
        "cnv_change_5_category",
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


def _add_ploidy_values(document_df: sql.DataFrame) -> sql.DataFrame:
    """Add the ploidy data to the given data frame.

    NOTE: Files with an lower/upper ploidy value of 0 are considered to be contaminated
    data and are removed from the document when calculating these ploidy values.

    STEPS:
        1) A ploidy data frame is created by aggregating the rows by file_id and
        copy_number. This aggregation takes the count of each such group thus giving the
        frequency of a given copy_number in a file.

        2a) A lower and upper ploidy are added wherein each of these represent the min
        and max copy_number of a subset of rows with the same file_id and frequency.
        Because this is not necessarily the values with the highest count/frequency for
        the file, these are not guaranteed to be the true ploidy/modal values.

        2b) Add row numbers to each subset of rows based on file_id ordered by the count
        of the copy_number occurrences in the file calculated in step 1. This should
        result in the group with the highest count or frequency with a row number of 1.

        3) Using the fact that the highest frequency value is associated with the row
        number of 1, the data frame is filtered to only those with said value. Thus the
        data frame is left with only the true modal values for each file.

    Args:
        document_df: The data frame containing the data from the cnv document. This must
            include the file_id and copy_number columns.

    Returns:
        A copy of the given document data frame with the upper_ploidy_number and
        lower_ploidy_number columns added.
    """
    ploidy_df = document_df.groupBy("file_id", "copy_number").agg(
        F.count("*").alias("count")
    )
    ploidy_window = sql.Window().partitionBy("file_id", "count")
    mode_window = (
        sql.Window().partitionBy("file_id").orderBy(F.col("count").desc_nulls_last())
    )
    ploidy_df = (
        ploidy_df.select(
            "file_id",
            F.min("copy_number").over(ploidy_window).alias("lower_ploidy_number"),
            F.max("copy_number").over(ploidy_window).alias("upper_ploidy_number"),
            F.row_number().over(mode_window).alias("row_number"),
        )
        .where(
            (F.col("row_number") == 1)
            & (F.col("lower_ploidy_number") != 0)
            & (F.col("upper_ploidy_number") != 0)
        )
        .select("file_id", "upper_ploidy_number", "lower_ploidy_number")
    )

    return document_df.join(ploidy_df, on="file_id", how="inner")


def _add_cnv_change_data(document_df: sql.DataFrame) -> sql.DataFrame:
    """
    Adds the cnv change related values to the data frame.

    Added columns:
        copy_number: This value is the raw copy_number value contained in the file for a
            given gene. It is used to calculate the cnv_change & cnv_change_5_category.

        ploidy_integer: This value is calculated from the lower and upper ploidy value
            of each file. These values are the minimum/maximum modal copy_number values
            respectively within each file. In most cases, this will be a single value
            (2), but in cases where the upper and lower are distinct, the ceiling value
            of the mean is used.

            NOTE: Files with a 0 upper or lower ploidy value are considered contaminated
            data and are removed from indexing. Thus ploidy values are always greater
            than 0.

        cnv_change: This value is based on the copy_number for a gene and its file's
            ploidy values. It is calculated as follows:
                - "Loss": copy_number is less than the lower ploidy.
                - None: copy_number is (inclusively) between the upper and lower ploidy.
                - "Gain": copy_number is greater than the upper ploidy.

        cnv_change_5_category: This value is based on the copy_number for a gene and its
            file's ploidy values. It is calculated as follows:
                - "Homozygous Deletion": copy_number equal to 0
                - "Loss": copy_number is less than the lower ploidy value.
                - None: copy_number is (inclusively) between the upper and lower ploidy.
                - "Gain": copy_number greater than the upper ploidy but less than double
                    the upper ploidy
                - "Amplification": copy_number is greater than or equal to double the
                    upper ploidy.

            NOTE: All cnv_change_5_category values of None, i.e. gene with no change,
            are not indexed and thus removed from the data.

    Args:
        document_df: The data frame containing the copy number data. This must include
            the file_id and copy_number columns

    Returns:
        A copy of the given data frame with the above columns added.
    """
    document_df = _add_ploidy_values(document_df)
    cnv_change = (
        F.when(F.col("copy_number") > F.col("upper_ploidy_number"), "Gain")
        .when(F.col("copy_number") < F.col("lower_ploidy_number"), "Loss")
        .otherwise(None)
        .alias("cnv_change")
    )
    cnv_change_5_category = (
        F.when(F.col("copy_number") == 0, "Homozygous Deletion")
        .when(F.col("copy_number") >= F.col("upper_ploidy_number") * 2, "Amplification")
        .when(F.col("copy_number") > F.col("upper_ploidy_number"), "Gain")
        .when(F.col("copy_number") < F.col("lower_ploidy_number"), "Loss")
        .otherwise(None)
        .alias("cnv_change_5_category")
    )
    mean_ploidy = (F.col("upper_ploidy_number") + F.col("lower_ploidy_number")) / 2

    return document_df.select(
        "*",
        cnv_change,
        cnv_change_5_category,
        F.ceil(mean_ploidy).cast("integer").alias("sample_ploidy_integer"),
    ).na.drop(subset="cnv_change_5_category")


class ASCATInputs(TypedDict):
    ascat_metadata_df: sql.DataFrame
    gene_model_df: sql.DataFrame


class ASCATBuilder(bases.InputBuilder[configuration.ASCATBuilder, ASCATInputs]):
    __slots__ = ("_document_dataframe_util",)

    def __init__(
        self,
        config: configuration.ASCATBuilder,
        spark_session: sql.SparkSession,
        document_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=ASCATInputs, output=build.DataFrame.ASCAT
        )

        self._document_dataframe_util = document_dataframe_util

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

        return document_df

    def _build_from_scratch(self, input_dfs: ASCATInputs) -> sql.DataFrame:
        """Builds the ASCAT dataframe

        ascat {}
        |---_id
        |---aliquot_id
        |---biotype
        |---canonical_transcript_id
        |---canonical_transcript_length
        |---canonical_transcript_length_cds
        |---canonical_transcript_length_genomic
        |---case_id
        |---chromosome
        |---cnv_change
        |---cnv_change_5_category
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
        document_df = self._build_document_df(
            r.file_id for r in ascat_metadata_df.select("file_id").toLocalIterator()
        )
        # Joining w/ gene model removes X/Y chromosomes & non-protein coding genes.
        # This should be done before calculating the cnv change value.
        ascat_df = document_df.join(gene_model_df, on="gene_id", how="inner")
        ascat_df = _add_cnv_change_data(ascat_df)
        ascat_df = ascat_df.join(ascat_metadata_df, on="file_id", how="inner")
        ascat_df = utils.add_canonical_transcript_lengths(ascat_df)
        ascat_df = _add_uuids(ascat_df)

        return ascat_df.select(
            "_id",
            "aliquot_id",
            "biotype",
            "canonical_transcript_id",
            "canonical_transcript_length",
            "canonical_transcript_length_cds",
            "canonical_transcript_length_genomic",
            "case_id",
            "chromosome",
            "cnv_change",
            "cnv_change_5_category",
            "cnv_id",
            "consequence_id",
            "copy_number",
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
            "sample_ploidy_integer",
            F.col("file_id").alias("src_file_id"),
            "start_position",
            "symbol",
            "synonyms",
            "transcripts",
            "uniprotkb_swissprot",
            F.col("workflow_type").alias("variant_caller"),
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
