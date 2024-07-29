import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import common
from mutation_indexer.constants import app, build

logging.basicConfig(format=app.LOG_FORMAT)


class CNVOccurrenceCentricInputs(TypedDict):
    cnv_df: sql.DataFrame


class CNVOccurrenceCentricBuilder(
    bases.IndexBuilder[common.IndexBuilder, CNVOccurrenceCentricInputs]
):
    def __init__(
        self,
        config: common.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=CNVOccurrenceCentricInputs,
            output=build.DataFrame.CNV_OCCURRENCE_CENTRIC,
        )

    def _build_from_scratch(
        self, input_dfs: CNVOccurrenceCentricInputs
    ) -> sql.DataFrame:
        return (
            input_dfs["cnv_df"]
            .select(
                F.struct(
                    "chromosome",
                    "cnv_change",
                    "cnv_id",
                    "consequence",
                    "end_position",
                    "gene_level_cn",
                    "ncbi_build",
                    "start_position",
                    "variant_status",
                ).alias("cnv"),
                F.explode("occurrence").alias("occurrence"),
            )
            .select(
                "occurrence.case",
                "cnv",
                F.col("occurrence.occurrence_id").alias("cnv_occurrence_id"),
            )
        )
