from typing import Tuple
from unittest import mock

import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import constants
from mutation_indexer.gene_expression import builders
from mutation_indexer.gene_expression import configuration
from tests.unit import utils
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
    ) -> builders.GeneExpressionInputs:
        value_df = self.spark_session.createDataFrame(
            values,  # type: ignore
            schema=self.values_schema,
        )
        case_df = self.spark_session.createDataFrame(
            cases,  # type: ignore
            schema=self.cases_schema,
        )

        return {"expression_value_df": value_df, "case_df": case_df}

    def arrange_config(self) -> configuration.IndexBuilder:
        config = mock.MagicMock(
            spec=configuration.IndexBuilder,
            backup=mock.MagicMock(mode=constants.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            partition_size=1,
            id_field="case_id",
        )

        return config

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionBuilder(
            config, spark_session, dataframe_util, mappings_loader
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__no_matching_file_ids(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self.arrange_inputs(
            (models.Value(file_id="file-1"),), (models.Case(file_id="file-2"),)
        )
        builder = builders.GeneExpressionBuilder(
            config, spark_session, dataframe_util, mappings_loader
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 0
        assert result_df.schema == self.final_schema
