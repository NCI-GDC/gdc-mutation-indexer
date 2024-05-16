from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


class DNAInputs(TypedDict):
    pass


class DNABuilder(bases.ResourceBuilder[viz.ResourceBuilder, DNAInputs]):
    def __init__(
        self, config: viz.ResourceBuilder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=DNAInputs,
            output=build.DataFrame.CIVIC_DNA,
        )

    def _build_from_scratch(self, input_dfs: DNAInputs) -> sql.DataFrame:
        df = self._load_resource(self._config.resources["data"])

        return df.select(
            "chromosome",
            "civic_gene_id",
            F.col("civic_var_id").alias("civic_variant_id"),
            "reference_allele",
            "start_position",
            F.col("alternative_allele").alias("tumor_allele"),
        )


class ProteinInputs(TypedDict):
    pass


class ProteinBuilder(bases.ResourceBuilder[viz.ResourceBuilder, ProteinInputs]):
    def __init__(
        self, config: viz.ResourceBuilder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=ProteinInputs,
            output=build.DataFrame.CIVIC_PROTEIN,
        )

    def _build_from_scratch(self, input_dfs: ProteinInputs) -> sql.DataFrame:
        df = self._load_resource(self._config.resources["data"])

        return df.select(
            "civic_gene_id",
            F.col("civic_var_id").alias("civic_variant_id"),
            F.col("hgvsp").alias("hgvsp_short"),
            F.col("hugo_symbol").alias("name"),
        )
