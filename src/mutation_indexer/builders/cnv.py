from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import common
from mutation_indexer.constants import build


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
        return ascat_df.groupBy("cnv_id", "case_id", "occurrence_id").agg(
            F.collect_list(
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
                )
            ).alias("observation")
        )

    def _build_occurrences(
        self, ascat_df: sql.DataFrame, case_df: sql.DataFrame
    ) -> sql.DataFrame:
        ascat_df.repartition(CNV_PARTITION * 10, "cnv_id", "case_id", "occurrence_id")
        observation_df = self._build_observations(ascat_df)

        return (
            observation_df.join(case_df, on="case_id")
            .groupBy("cnv_id")
            .agg(
                F.collect_list(
                    F.struct(
                        F.struct(*case_df.columns, "observation").alias("case"),
                        "occurrence_id",
                    )
                ).alias("occurrence"),
            )
        )

    def _build_from_scratch(self, input_dfs: CNVInputs) -> sql.DataFrame:
        ascat_df = input_dfs["ascat_df"].repartition(CNV_PARTITION, "cnv_id")
        case_df = input_dfs["case_df"].where(
            F.array_contains("available_variation_data", F.lit("cnv"))
        )
        occurrence_df = self._build_occurrences(ascat_df, case_df)
        cnv_df = ascat_df.drop_duplicates(subset=["cnv_id"]).select(
            "chromosome",
            "cnv_change",
            "cnv_id",
            "end_position",
            "gene_level_cn",
            "ncbi_build",
            "start_position",
            F.array(
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

        return cnv_df.join(occurrence_df, on="cnv_id").select(
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
