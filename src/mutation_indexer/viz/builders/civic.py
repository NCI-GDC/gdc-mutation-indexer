from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as pyspark_functions

from mutation_indexer import builders
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration


class DNAInputs(TypedDict):
    pass


class DNABuilder(builders.ResourceBuilder[configuration.CIVIC.DNABuilder, DNAInputs]):
    def __init__(
        self, config: configuration.CIVIC.DNABuilder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=DNAInputs,
            output=build.DataFrame.CIVIC_DNA,
        )

    def _build_from_scratch(self, input_dfs: DNAInputs) -> sql.DataFrame:
        df = self._load_resource_data()

        return df.select(
            "chromosome",
            "civic_gene_id",
            pyspark_functions.col("civic_var_id").alias("civic_variant_id"),
            "reference_allele",
            "start_position",
            pyspark_functions.col("alternative_allele").alias("tumor_allele"),
        )


class ProteinInputs(TypedDict):
    pass


class ProteinBuilder(
    builders.ResourceBuilder[configuration.CIVIC.ProteinBuilder, ProteinInputs]
):
    def __init__(
        self,
        config: configuration.CIVIC.ProteinBuilder,
        spark_session: sql.SparkSession,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=ProteinInputs,
            output=build.DataFrame.CIVIC_PROTEIN,
        )

    def _build_from_scratch(self, input_dfs: ProteinInputs) -> sql.DataFrame:
        df = self._load_resource_data()

        return df.select(
            "civic_gene_id",
            pyspark_functions.col("civic_var_id").alias("civic_variant_id"),
            pyspark_functions.col("hgvsp").alias("hgvsp_short"),
            pyspark_functions.col("hugo_symbol").alias("name"),
        )
