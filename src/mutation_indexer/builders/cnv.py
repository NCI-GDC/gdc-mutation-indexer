import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import common
from mutation_indexer.constants import build

logger = logging.getLogger(__name__)


class CNVInputs(TypedDict):
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame


CNV_PARTITION = 24


class CNVConfig(common.Builder):
    final_partitions: int = CNV_PARTITION


class CNVBuilder(bases.InputBuilder[common.Builder, CNVInputs]):
    def __init__(self, config: common.Builder, spark_session: sql.SparkSession) -> None:
        super().__init__(
            config, spark_session, input_type=CNVInputs, output=build.DataFrame.CNV
        )

    def _build_observations(self, ascat_df: sql.DataFrame) -> sql.DataFrame:
        return (
            ascat_df.select(
                "cnv_id",
                "case_id",
                "occurrence_id",
                F.struct(
                    "observation_id",
                    # F.struct(
                    #     # TODO: add this sample data to ascat
                    #     # "tumor_sample_barcode",
                    #     "tumor_sample_uuid",
                    # ).alias("sample"),
                    "src_file_id",
                    F.struct("variant_caller").alias("variant_calling"),
                    "variant_status",
                ).alias("observation"),
            )
            .groupBy("cnv_id", "case_id", "occurrence_id")
            .agg(F.collect_list("observation").alias("observation"))
        )

    def _build_occurrences(
        self, ascat_df: sql.DataFrame, case_df: sql.DataFrame
    ) -> sql.DataFrame:
        observation_df = self._build_observations(ascat_df)

        return (
            observation_df.join(case_df, on="case_id", how="left")
            .select(
                "cnv_id",
                F.struct(
                    F.struct(*case_df.columns, "observation").alias("case"),
                    "occurrence_id",
                ).alias("occurrence"),
            )
            .groupBy("cnv_id")
            .agg(F.collect_list("occurrence").alias("occurrence"))
        )

    def _build_from_scratch(self, input_dfs: CNVInputs) -> sql.DataFrame:
        ascat_df = input_dfs["ascat_df"].repartition(
            CNV_PARTITION * 10, "cnv_id", "case_id", "occurrence_id"
        )
        case_df = input_dfs["case_df"].repartition(CNV_PARTITION * 10, "case_id")
        occurrence_df = self._build_occurrences(ascat_df, case_df)
        cnv_df = (
            ascat_df.groupBy("cnv_id")
            .agg(
                # These values are unique across all rows in the ascat df with the same
                # cnv_id
                F.first(
                    F.struct(
                        "chromosome",
                        "cnv_change",
                        "end_position",
                        "gene_level_cn",
                        "ncbi_build",
                        "start_position",
                    ),
                ).alias("cnv"),
                F.collect_set(
                    F.struct(
                        "consequence_id",
                        F.struct(
                            "biotype",
                            "gene_id",
                            "is_cancer_gene_census",
                            "symbol",
                        ).alias("gene"),
                    )
                ).alias("consequence"),
            )
            .select("cnv_id", "cnv.*", "consequence")
        )

        return cnv_df.join(occurrence_df, on="cnv_id", how="left").select(
            "chromosome",
            "consequence",
            "cnv_change",
            "cnv_id",
            "end_position",
            "gene_level_cn",
            "ncbi_build",
            "occurrence",
            "start_position",
        )
