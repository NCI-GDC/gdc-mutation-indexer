from typing import Dict, Tuple
from unittest import mock

import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def values_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Value.FINAL.load()


@pytest.fixture(scope="class")
def cases_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Case.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.GeneExpression.FINAL.load()


class TestGeneExpressionBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        values_schema: types.StructType,
        cases_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.values_schema = values_schema
        self.cases_schema = cases_schema
        self.final_schema = final_schema

    def arrange_inputs(
        self,
        values: Tuple[models.Value, ...] = (models.Value(),),
        cases: Tuple[models.Case, ...] = (models.Case(),),
    ) -> Dict[str, sql.DataFrame]:
        value_df = self.spark_session.createDataFrame(
            values,  # type: ignore
            schema=self.values_schema,
        )
        case_df = self.spark_session.createDataFrame(
            cases,  # type: ignore
            schema=self.cases_schema,
        )

        return {"expression_value_df": value_df, "case_df": case_df}

    def test__build__single_row(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionBuilder(config, sql_context)

        builder.build(**inputs)

        result_df = builder.gene_expression

        assert result_df
        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__no_matching_file_ids(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs(
            (models.Value(file_id="file-1"),), (models.Case(file_id="file-2"),)
        )
        builder = builders.GeneExpressionBuilder(config, sql_context)

        builder.build(**inputs)

        result_df = builder.gene_expression

        assert result_df
        assert result_df.count() == 0
        assert result_df.schema == self.final_schema
