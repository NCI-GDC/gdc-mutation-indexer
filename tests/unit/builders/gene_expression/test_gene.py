from collections.abc import Iterable, Mapping
from unittest import mock

import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders.gene_expression import gene
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def value_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Gene.FINAL.load()


class TestGeneSQLBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        value_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._value_schema = value_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> gene_expression.Builder:
        return mock.MagicMock(
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def _arrange_database(self) -> mock.MagicMock:
        return mock.MagicMock(spec=sqlite.SQLiteDatabase)

    def _arrange_inputs(
        self, values: Iterable[models.ExpressionValue] = (models.ExpressionValue(),)
    ) -> Mapping[str, sql.DataFrame]:
        return {
            "expression_value_df": self._create_dataframe(values, self._value_schema)
        }

    def test__build__single_row(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()
        inputs = self._arrange_inputs()
        builder = gene.GeneSQLBuilder(config, mock.MagicMock(), database)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_transformed(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()

        values = (
            models.ExpressionValue(gene_id="gene-0", symbol="GENE0", uqfpkm=34.5),
            models.ExpressionValue(gene_id="gene-0", symbol="GENE0", uqfpkm=4.8),
            models.ExpressionValue(gene_id="gene-1", symbol="GENE1", uqfpkm=0.4),
        )
        inputs = self._arrange_inputs(values=values)
        builder = gene.GeneSQLBuilder(config, mock.MagicMock(), database)

        result_df = builder.build(**inputs)
        result_rows = frozenset(tuple(r) for r in result_df.collect())

        assert result_rows == frozenset((("gene-0", "GENE0"), ("gene-1", "GENE1")))

    def test__build__data_written(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()
        inputs = self._arrange_inputs()
        builder = gene.GeneSQLBuilder(config, mock.MagicMock(), database)

        _ = builder.build(**inputs)

        database.write.assert_called_once()
