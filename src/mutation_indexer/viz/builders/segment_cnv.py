from collections.abc import Iterable
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as pyspark_functions
from pyspark.sql import types

from mutation_indexer import builders, indexd_utils, schemas
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

UUIDS_STRUCT = schemas.load_schema("builders/segment_cnv/uuids.yaml")
SEGMENT_CNV_DOCUMENT_SCHEMA = "builders/segment_cnv/segment_cnv_document.yaml"
CHROMOSOME_REGEX_PATTERN = r"chr(\d+)"


@pyspark_functions.udf(returnType=UUIDS_STRUCT)
def _generate_uuids(
    chromosome: str,
    start_position: int,
    end_position: int,
    cnv_change_5_category: str,
    case_id: str,
    aliquot_id: str,
) -> dict[str, str]:
    """Generates uuids required for resultant dataframe of SegmentCNVBuilder.

    This udf will add the following uuids:
        - segment_cnv_id
        - occurrence_id
        - observation_id
    """
    segment_cnv_id = utils.generate_uuid5(
        chromosome, start_position, end_position, cnv_change_5_category
    )
    occurrence_id = utils.generate_uuid5(segment_cnv_id, case_id)
    observation_id = utils.generate_uuid5(segment_cnv_id, case_id, aliquot_id)

    return {
        "segment_cnv_id": segment_cnv_id,
        "occurrence_id": occurrence_id,
        "observation_id": observation_id,
    }


class SegmentCNVInputs(TypedDict):
    segment_cnv_metadata_df: sql.DataFrame


class SegmentCNVBuilder(
    builders.InputBuilder[configuration.SegmentCNVBuilder, SegmentCNVInputs]
):
    def __init__(
        self,
        config: configuration.SegmentCNVBuilder,
        spark_session: sql.SparkSession,
        document_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        """Input dataframe builder that gathers copy number segment file data.

        Given the copy number segment file ids as input, the file contents are retrieved
        from indexd. The segment length is calculated using the start and end positions
        of the chromosome, and then the cnv_change and cnv_change_5_category fields are
        calculated using a weighted mode.
        """
        super().__init__(
            config,
            spark_session,
            input_type=SegmentCNVInputs,
            output=build.DataFrame.SEGMENT_CNV,
        )

        self._document_dataframe_util = document_dataframe_util

    def _filter_by_chromosome(self, document_df: sql.DataFrame) -> sql.DataFrame:
        """Filters out rows in dataframe based on chromosome value.

        This function will only keep chromosomes that have an integer value between
        1 and 22, inclusive.

        STEPS:
            1) Create a temporary column that contains the extracted integer portion
            of the chromosome value.
            2) Filter the dataframe to only keep rows that have an extracted chromosome
            integer between 1 and 22, inclusive.
            3) Overwrite the original chromosome column with the values of the temporary
            chromosome integer column, casted back to a string type.
            4) Drop the temporary chromosome integer column.

        """
        chromosome_field = "chromosome"
        chromosome_integer_field = "chromosome_integer"
        document_df = (
            document_df.withColumn(
                chromosome_integer_field,
                pyspark_functions.regexp_extract(
                    chromosome_field, CHROMOSOME_REGEX_PATTERN, 1
                ).cast(types.IntegerType()),
            )
            .filter(pyspark_functions.col(chromosome_integer_field).between(1, 22))
            .withColumn(
                chromosome_field,
                pyspark_functions.col(chromosome_integer_field).cast(types.StringType()),
            )
            .drop(chromosome_integer_field)
        )

        return document_df

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        """Read files into dataframe and filter out unwanted data."""
        document_df = self._document_dataframe_util.get_dataframe(
            doc_ids, schema=schemas.load_schema(SEGMENT_CNV_DOCUMENT_SCHEMA)
        )
        document_df = self._filter_by_chromosome(document_df)
        document_df = document_df.select(
            pyspark_functions.col("did").alias("file_id"),
            "copy_number",
            "chromosome",
            "start_position",
            "end_position",
        )

        return document_df

    def _add_ploidy_values(self, document_df: sql.DataFrame) -> sql.DataFrame:
        """Calculates length-weighted mode and mean ploidy.

        NOTE: Files with an lower/upper ploidy value of 0 are considered to be
        contaminated data and are removed from the document when calculating these
        ploidy values.

        For the length-weighted mode, the copy_numbers are used as the values, and
        the segment lengths are used as the weights.

        STEPS:
            1) A ploidy data frame is created by aggregating the rows by file_id and
            copy_number and summing up the lengths per group. This aggregation gives
            the total weight for each group, the file_id and copy_number combination.

            2a) A lower and upper ploidy are added wherein each of these represent the
            min and max copy_number of a subset of rows with the same file_id and
            total weight. Because this is not necessarily the values with the
            highest total weight for the file, these are not guaranteed to be the
            true ploidy/modal values.

            2b) Add row numbers to each subset of rows based on file_id ordered by the
            total weight of the copy_number occurrences in the file calculated in step
            1. This should result in the group with the highest length-weighted mode
            with a row number of 1.

            3) Using the fact that the highest total weight value is associated with the
            row number of 1, the data frame is filtered to only those with said value.
            Thus the data frame is left with only the true modal values for each file.
        """
        ploidy_df = document_df.groupBy("file_id", "copy_number").agg(
            pyspark_functions.sum("length").alias("total_weight")
        )
        ploidy_window = sql.Window().partitionBy("file_id", "total_weight")
        mode_window = (
            sql.Window()
            .partitionBy("file_id")
            .orderBy(pyspark_functions.col("total_weight").desc_nulls_last())
        )
        ploidy_df = (
            ploidy_df.select(
                "file_id",
                pyspark_functions.min("copy_number")
                .over(ploidy_window)
                .alias("lower_ploidy_number"),
                pyspark_functions.max("copy_number")
                .over(ploidy_window)
                .alias("upper_ploidy_number"),
                pyspark_functions.row_number().over(mode_window).alias("row_number"),
            )
            .where(
                (pyspark_functions.col("row_number") == 1)
                & (pyspark_functions.col("lower_ploidy_number") != 0)
                & (pyspark_functions.col("upper_ploidy_number") != 0)
            )
            .select("file_id", "upper_ploidy_number", "lower_ploidy_number")
        )
        document_df = document_df.join(ploidy_df, on="file_id", how="inner")

        return document_df

    def _add_cnv_change_data(self, document_df: sql.DataFrame) -> sql.DataFrame:
        """Adds the cnv change related values to the dataframe.

        Added columns:
            sample_ploidy_integer: This value is calculated from the lower and upper
                ploidy value of each file. These values are the minimum/maximum modal
                copy_number values respectively within each file. In most cases, this
                will be a single value, but in cases where the upper and lower are
                distinct, the ceiling value of the mean is used.

                NOTE: Files with a 0 upper or lower ploidy value are considered
                contaminated data and are removed from indexing. Thus ploidy values are
                always greater than 0.

            cnv_change: This value is based on the copy_number and its file's ploidy
                values. It is calculated as follows:
                    - "Loss": copy_number is less than the lower ploidy.
                    - None: copy_number is (inclusively) between the upper and lower
                        ploidy.
                    - "Gain": copy_number is greater than the upper ploidy.

            cnv_change_5_category: This value is based on the copy_number and its
                file's ploidy values. It is calculated as follows:
                    - "Homozygous Deletion": copy_number equal to 0
                    - "Loss": copy_number is less than the lower ploidy value.
                    - None: copy_number is (inclusively) between the upper and lower
                        ploidy.
                    - "Gain": copy_number greater than the upper ploidy but less than
                        double.
                        the upper ploidy
                    - "Amplification": copy_number is greater than or equal to double
                        the upper ploidy.

                NOTE: All cnv_change_5_category values of None are not indexed and thus
                removed from the data.
        """
        document_df = self._add_ploidy_values(document_df)
        cnv_change = (
            pyspark_functions.when(
                pyspark_functions.col("copy_number")
                > pyspark_functions.col("upper_ploidy_number"),
                "Gain",
            )
            .when(
                pyspark_functions.col("copy_number")
                < pyspark_functions.col("lower_ploidy_number"),
                "Loss",
            )
            .otherwise(None)
            .alias("cnv_change")
        )
        cnv_change_5_category = (
            pyspark_functions.when(
                pyspark_functions.col("copy_number") == 0, "Homozygous Deletion"
            )
            .when(
                pyspark_functions.col("copy_number")
                >= pyspark_functions.col("upper_ploidy_number") * 2,
                "Amplification",
            )
            .when(
                pyspark_functions.col("copy_number")
                > pyspark_functions.col("upper_ploidy_number"),
                "Gain",
            )
            .when(
                pyspark_functions.col("copy_number")
                < pyspark_functions.col("lower_ploidy_number"),
                "Loss",
            )
            .otherwise(None)
            .alias("cnv_change_5_category")
        )
        mean_ploidy = (
            pyspark_functions.col("upper_ploidy_number")
            + pyspark_functions.col("lower_ploidy_number")
        ) / 2
        document_df = document_df.select(
            "*",
            cnv_change,
            cnv_change_5_category,
            pyspark_functions.ceil(mean_ploidy)
            .cast(types.IntegerType())
            .alias("sample_ploidy_integer"),
        ).na.drop(subset="cnv_change_5_category")

        return document_df

    def _add_uuids(self, document_df: sql.DataFrame) -> sql.DataFrame:
        """Add uuid column to dataframe."""
        uuids = _generate_uuids(
            "chromosome",
            "start_position",
            "end_position",
            "cnv_change_5_category",
            "case_id",
            "aliquot_id",
        )
        document_df = document_df.withColumn("uuids", uuids)
        document_df = document_df.select("*", "uuids.*")

        return document_df

    def _add_segment_length(self, document_df: sql.DataFrame) -> sql.DataFrame:
        """Adds segment length column to dataframe.

        The segment length will be end_position - start_position + 1.
        """
        document_df = document_df.withColumn(
            "length",
            pyspark_functions.col("end_position")
            - pyspark_functions.col("start_position")
            + 1,
        )

        return document_df

    def _build_from_scratch(self, input_dfs: SegmentCNVInputs) -> sql.DataFrame:
        """Builds the SegmentCNV dataframe.

        segment_cnv {}
        |---segment_cnv_id
        |---occurrence_id
        |---observation_id
        |---chromosome
        |---length
        |---start_position
        |---end_position
        |---cnv_change
        |---cnv_change_5_category

        """
        segment_cnv_metadata_df = input_dfs["segment_cnv_metadata_df"]
        document_df = self._build_document_df(
            row.file_id for row in segment_cnv_metadata_df.select("file_id").toLocalIterator()
        )
        segment_cnv_df = document_df.join(segment_cnv_metadata_df, on="file_id", how="inner")
        segment_cnv_df = self._add_segment_length(segment_cnv_df)
        segment_cnv_df = self._add_cnv_change_data(segment_cnv_df)
        segment_cnv_df = self._add_uuids(segment_cnv_df)
        segment_cnv_df = segment_cnv_df.select(
            "segment_cnv_id",
            "case_id",
            "aliquot_id",
            pyspark_functions.col("file_id").alias("src_file_id"),
            "occurrence_id",
            "observation_id",
            "copy_number",
            "chromosome",
            "start_position",
            "end_position",
            "length",
            "cnv_change",
            "cnv_change_5_category",
            "sample_ploidy_integer",
            pyspark_functions.col("workflow_type").alias("variant_caller"),
            pyspark_functions.lit("Tumor Only").alias("variant_status"),
        )

        return segment_cnv_df
