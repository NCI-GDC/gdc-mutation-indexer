from typing import TypedDict

from pyspark import sql

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import common
from mutation_indexer.constants import build
from mutation_indexer.es_utils import DataFrameUtil, MappingsLoader


class CNVCentricInputs(TypedDict):
    cnv_df: sql.DataFrame


class CNVCentricBuilder(bases.IndexBuilder[common.IndexBuilder, CNVCentricInputs]):
    def __init__(
        self,
        config: common.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: DataFrameUtil,
        mappings_loader: MappingsLoader,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=CNVCentricInputs,
            output=build.DataFrame.CNV_CENTRIC,
        )

    def _build_from_scratch(self, input_dfs: CNVCentricInputs) -> sql.DataFrame:
        return input_dfs["cnv_df"].select(
            "chromosome",
            "cnv_change",
            "cnv_id",
            "consequence",
            "end_position",
            "gene_level_cn",
            "ncbi_build",
            "occurrence",
            "start_position",
        )
