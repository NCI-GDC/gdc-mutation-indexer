from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import es_utils
from mutation_indexer.builders.gene_expression import primary_aliquot
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def input_file_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.PrimaryAliquot.FILE.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.PrimaryAliquot.FINAL.load()


class TestPrimaryAliquotBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        input_file_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.input_file_schema = input_file_schema
        self.final_schema = final_schema

    def arrange_config(self) -> gene_expression.Builder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=gene_expression.Builder, projects=(), is_cached=False, backup=backup
        )

    def arrange_es_dataframe_util(
        self, data: tuple[models.File, ...]
    ) -> es_utils.DataFrameUtil:
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)

        dataframe_util.read.return_value = self.spark_session.createDataFrame(
            data,  # type: ignore
            schema=self.input_file_schema,
        )

        return dataframe_util

    def arrange_builder(
        self, data: tuple[models.File, ...] = (models.File(),)
    ) -> primary_aliquot.PrimaryAliquotBuilder:
        config = self.arrange_config()
        util = self.arrange_es_dataframe_util(data)
        spark_session = mock.MagicMock(spec=sql.SparkSession)

        return primary_aliquot.PrimaryAliquotBuilder(config, spark_session, util)

    def test__build__single_row(self) -> None:
        builder = self.arrange_builder()

        result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        es_file = models.File()
        es_case = es_file.cases[0]
        builder = self.arrange_builder((es_file,))

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == es_file.file_id
        assert result_row.case_id == es_case.case_id
        assert result_row.submitter_id == es_case.submitter_id
