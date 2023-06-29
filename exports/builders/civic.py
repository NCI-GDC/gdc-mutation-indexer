from importlib import resources
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from exports.builders import bases
from exports.configuration.builders import viz
from exports.constants import build


class PROTInputs(TypedDict):
    ...


class PROTBuilder(bases.InputBuilder[viz.PROTBuilder, PROTInputs]):
    __slots__ = ()

    def __init__(
        self, config: viz.PROTBuilder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=PROTInputs,
            output=build.DataFrame.CIVIC_PROT,
        )

    def _build_from_scratch(self, input_dfs: PROTInputs) -> sql.DataFrame:
        with resources.path(
            self._config.data_package, self._config.data_resource
        ) as file_path:
            df = self._spark_session.read.csv(str(file_path), sep="\t", header=True)

        return df.select(
            F.col("hgvsp").alias("hgvsp_short"),
            F.col("hugo_symbol").alias("name"),
            F.col("civic_gene_id").alias("prot_civic_gene_id"),
            F.col("civic_var_id").alias("prot_civic_var_id"),
        )


class DNAInputs(TypedDict):
    ...


class DNABuilder(bases.InputBuilder[viz.DNABuilder, DNAInputs]):
    __slots__ = ()

    def __init__(self, config: viz.DNABuilder, spark_session: sql.SparkSession) -> None:
        super().__init__(
            config,
            spark_session,
            input_type=DNAInputs,
            output=build.DataFrame.CIVIC_DNA,
        )

    def _build_from_scratch(self, input_dfs: DNAInputs) -> sql.DataFrame:
        with resources.path(
            self._config.data_package, self._config.data_resource
        ) as file_path:
            df = self._spark_session.read.csv(str(file_path), sep="\t", header=True)

        return df.select(
            "chromosome",
            F.col("civic_gene_id").alias("dna_civic_gene_id"),
            F.col("civic_var_id").alias("dna_civic_var_id"),
            "reference_allele",
            F.col("start_position").cast(types.IntegerType()),
            F.col("alternative_allele").alias("tumor_allele"),
        )
