from typing import TypedDict

from pyspark import sql

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite


class GeneInputs(TypedDict):
    expression_value_df: sql.DataFrame


class GeneBuilder(bases.SQLiteBuilder[gene_expression.Builder, GeneInputs]):
    def __init__(
        self,
        config: gene_expression.Builder,
        spark_session: sql.SparkSession,
        database: sqlite.SQLiteDatabase,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            database,
            input_type=GeneInputs,
            output=build.DataFrame.GENE,
        )

    @property
    def _create(self) -> str:
        return "CREATE TABLE IF NOT EXISTS genes(gene_id TEXT PRIMARY KEY, symbol TEXT)"

    @property
    def _insert(self) -> str:
        return "INSERT INTO genes (gene_id, symbol) VALUES (?, ?)"

    def _build_from_scratch(self, input_dfs: GeneInputs) -> sql.DataFrame:
        return input_dfs["expression_value_df"].select("gene_id", "symbol").distinct()
