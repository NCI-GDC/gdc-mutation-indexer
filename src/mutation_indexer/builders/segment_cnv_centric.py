"""Builds the segment_cnv_centric dataframe."""
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import es_utils
from mutation_indexer.builders import bases, utils
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


class IndexBuilderInputs(TypedDict):
    segment_cnv_df: sql.DataFrame
    case_df: sql.DataFrame


class IndexBuilder(
    bases.IndexBuilder[viz.SegmentCNVCentricBuilder, IndexBuilderInputs]
):
    INDEX_NAME = "segment_cnv_centric"

    def __init__(
        self,
        config: viz.SegmentCNVCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ):
        """Builds segment_cnv_centric dataframe given case and segment_cnv dataframes.

        segment_cnv{}
            |____ occurrence[]
                        |_____ case{}
                                    |____ observation[]
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=IndexBuilderInputs,
            output=build.DataFrame.SEGMENT_CNV_CENTRIC,
        )

    def _truncate_occurrence_outliers(
        self, segment_cnv_centric_df: sql.DataFrame, threshold: int
    ) -> sql.DataFrame:
        """Filters dataframe rows based on percentile rank of occurrence array size.

        Keeps rows that have occurrence array lengths that have a percentile ranking
        under the configured threshold.
        """
        segment_cnv_centric_df = utils.filter_arrays_by_relative_size(
            segment_cnv_centric_df, "occurrence", threshold
        )

        return segment_cnv_centric_df

    def _build_observation_df(self, segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
        """Builds the observation dataframe from the segment_cnv dataframe.

        observation[]
        |____ observation{}
                |____ observation_id
                |____ variant_status
                |____ variant_calling {}
                        |____ variant_caller
        """
        obs_cols = (
            F.col("observation_id"),
            F.col("copy_number"),
            F.col("sample_ploidy_integer"),
            F.col("src_file_id"),
            F.struct("variant_caller").alias("variant_calling"),
            F.col("variant_status"),
        )
        obs_df = (
            segment_cnv_df.select(
                "segment_cnv_id",
                "case_id",
                "occurrence_id",
                F.struct(*obs_cols).alias("observation"),
            )
            .groupby("segment_cnv_id", "case_id", "occurrence_id")
            .agg(F.collect_set("observation").alias("observation"))
        )

        return obs_df

    def _build_occurrence_df(
        self, segment_cnv_df: sql.DataFrame, case_df: sql.DataFrame
    ) -> sql.DataFrame:
        """Builds the occurrence dataframe from the segment_cnv dataframe.

        This assumes case_id is already present in segment_cnv_df.

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        assert "case_id" in segment_cnv_df.columns

        obs_df = self._build_observation_df(segment_cnv_df)
        occurrence_df = (
            case_df.join(obs_df, on=["case_id"], how="left")
            .select(
                "segment_cnv_id",
                F.struct(
                    "occurrence_id",
                    F.struct("observation", *case_df.columns).alias("case"),
                ).alias("occurrence"),
            )
            .groupby("segment_cnv_id")
            .agg(F.collect_set("occurrence").alias("occurrence"))
        )

        return occurrence_df

    def _filter_segment_cnv_df(self, segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
        """Remove extraneous columns and drop duplicate rows.

        Keep columns in segment_cnv_df that are only necessary in final dataframe.
        Drop duplicate rows based on segment_cnv_id, as the final dataframe shall
        only have one row per segment.
        """
        segment_cnv_df = segment_cnv_df.select(
            "segment_cnv_id",
            "start_position",
            "end_position",
            "length",
            "chromosome",
            "cnv_change",
            "cnv_change_5_category",
        )
        segment_cnv_df = segment_cnv_df.drop_duplicates(subset=["segment_cnv_id"])

        return segment_cnv_df

    def _build_from_scratch(self, input_dfs: IndexBuilderInputs) -> sql.DataFrame:
        """Builds segment_cnv_centric dataframe.

        STEPS:
            1) Build the occurrence dataframe from the segment and case dataframes.

            2) Select the columns from segment_cnv that will be in the root level of
            the final dataframe, as well as ensure segment_cnv_df has one row per
            segment. This is needed because the incoming segment_cnv dataframe might
            have two rows that describe the same segment but occurred in different
            cases.

            Example segment_cnv_df:

            cols: | segment_cnv_id | occurrence_id | observation_id | case_id |
            row1: | segment_cnv-0  | occ-0         | obs-0          | case-0  |
            row2: | segment_cnv-0  | occ-1         | obs-1          | case-1  |

            In step 1, all the occurrences for the same segment_cnv will be grouped
            together. Prior to joining the segment_cnv dataframe with the
            occurrence_df, we can drop the rows with duplicate segment_cnv_id, as we
            only want to have 1 row per segment_cnv in the final dataframe. This will
            also reduce the number of joins and size of the final dataframe.

            3) Join the segment_cnv dataframe with the occurrence dataframe.

            4) Truncate the outlier occurrences per row, if configured.
        """
        segment_cnv_df = input_dfs["segment_cnv_df"]
        case_df = input_dfs["case_df"]

        occurrence_df = self._build_occurrence_df(segment_cnv_df, case_df)
        segment_cnv_df = self._filter_segment_cnv_df(segment_cnv_df)
        segment_cnv_centric_df = segment_cnv_df.join(
            occurrence_df, on="segment_cnv_id", how="left"
        )
        segment_cnv_centric_df = self._truncate_occurrence_outliers(
            segment_cnv_centric_df, self._config.occurrences_threshold
        )

        return segment_cnv_centric_df


# TODO: add build() and load() function to this class
# create protocol that defines build and load that can be used in driver
# create adapter class that has build() and load()
# build() will call _build_from_scratch() (need to overwrite _write method so it doesn't get written to es automatically)
# load() will call _write() which will write to elasticsearch


# the alternative:
# just create another list of NEW STYLE index builders, and in driver, populate that list with this and put it as
# another property in the exporter

# then, just call build() on this list in the run() function
