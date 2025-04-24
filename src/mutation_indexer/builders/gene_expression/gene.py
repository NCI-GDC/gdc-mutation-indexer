"""A module for building gene data for the Gene Expression Service."""

from typing import TypedDict

from pyspark import sql

from mutation_indexer.builders import bases
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite
from mutation_indexer.gene_expression import configuration


class GeneSQLInputs(TypedDict):
    expression_value_df: sql.DataFrame


class GeneSQLBuilder(bases.SQLiteBuilder[configuration.GeneSQLBuilder, GeneSQLInputs]):
    def __init__(
        self,
        config: configuration.GeneSQLBuilder,
        spark_session: sql.SparkSession,
        database: sqlite.SQLiteDatabase,
    ) -> None:
        """A builder for loading the genes table in the gene expression database.

        Args:
            config: The configuration for the builder this run.
            spark_session: The spark session related to this run.
            database: The SQLite database to which the data needs to be written.
        """
        super().__init__(
            config,
            spark_session,
            database,
            input_type=GeneSQLInputs,
            output=build.DataFrame.GENE_SQL,
        )

    @property
    def _create(self) -> str:
        return "CREATE TABLE IF NOT EXISTS genes(gene_id TEXT PRIMARY KEY, symbol TEXT)"

    @property
    def _insert(self) -> str:
        return "INSERT INTO genes (gene_id, symbol) VALUES (?, ?)"

    def _build_from_scratch(self, input_dfs: GeneSQLInputs) -> sql.DataFrame:
        """Builds the gene SQL data

        gene_sql {}
        |---gene_id
        +---symbol
        """
        return input_dfs["expression_value_df"].select("gene_id", "symbol").distinct()
