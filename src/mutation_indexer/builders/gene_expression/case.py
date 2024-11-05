from typing import TypedDict

from pyspark import sql

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite


class CaseInputs(TypedDict):
    expression_value_df: sql.DataFrame


class CaseBuilder(bases.SQLiteBuilder[gene_expression.Builder, CaseInputs]):
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
            input_type=CaseInputs,
            output=build.DataFrame.CASE,
        )

    @property
    def _create(self) -> str:
        return "CREATE TABLE cases(case_id TEXT PRIMARY KEY, submitter_id TEXT)"

    @property
    def _insert(self) -> str:
        return "INSERT INTO cases (case_id, submitter_id) VALUES (?, ?)"

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        return (
            input_dfs["expression_value_df"]
            .select("case_id", "submitter_id")
            .distinct()
        )
