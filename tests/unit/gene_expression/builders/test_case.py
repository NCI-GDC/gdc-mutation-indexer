import marshal
from collections.abc import Callable, Iterable, Mapping
from typing import IO
from unittest import mock

import more_itertools
import mypy_boto3_s3 as s3
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite
from mutation_indexer.gene_expression import builders, configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def value_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Case.FINAL.load()


class TestCaseBuilder:
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

    def _arrange_config(self) -> configuration.CaseBuilder:
        return mock.MagicMock(
            spec=configuration.CaseBuilder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            destination=mock.MagicMock(bucket="bucket-test-gdc", key="test/case.bin"),
        )

    def _arrange_s3_client(
        self, validate: Callable[..., None] = lambda *args, **kwargs: None
    ) -> mock.MagicMock:
        return mock.MagicMock(
            spec=s3.Client, upload_fileobj=mock.MagicMock(side_effect=validate)
        )

    def _arrange_inputs(
        self, values: Iterable[models.ExpressionValue] = (models.ExpressionValue(),)
    ) -> Mapping[str, sql.DataFrame]:
        return {"expression_value_df": self._create_dataframe(values, self._value_schema)}

    def _arrange_builder(
        self, config: configuration.CaseBuilder, s3_client: s3.Client
    ) -> builders.CaseBuilder:
        return builders.CaseBuilder(config, mock.MagicMock(), s3_client)

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        s3_client = self._arrange_s3_client()
        inputs = self._arrange_inputs()
        builder = self._arrange_builder(config, s3_client)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_ordered_by_case_id(self) -> None:
        case_ids = ("case-3", "case-1", "case-0", "case-2", "case-1")
        values = (models.ExpressionValue(case_id=i) for i in case_ids)
        config = self._arrange_config()
        s3_client = self._arrange_s3_client()
        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_client)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cases == sorted(frozenset(case_ids))

    def test__build__data_uploaded(self) -> None:
        config = self._arrange_config()
        values = (
            models.ExpressionValue(case_id="case-2"),
            models.ExpressionValue(case_id="case-1"),
        )

        def validate_upload(data: IO[bytes], **kwargs) -> None:
            assert kwargs["Bucket"] == config.destination.bucket
            assert kwargs["Key"] == config.destination.key
            assert marshal.load(data) == ["case-1", "case-2"]

        s3_client = self._arrange_s3_client(validate_upload)
        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_client)

        _ = builder.build(**inputs)

        s3_client.upload_fileobj.assert_called_once()


@pytest.fixture(scope="class")
def sql_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Case.SQL.load()


class TestCaseSQLBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        value_schema: types.StructType,
        sql_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._value_schema = value_schema
        self._final_schema = sql_schema

    def _arrange_config(self) -> configuration.CaseSQLBuilder:
        return mock.MagicMock(
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def _arrange_database(self) -> mock.MagicMock:
        return mock.MagicMock(spec=sqlite.SQLiteDatabase)

    def _arrange_inputs(
        self, values: Iterable[models.ExpressionValue] = (models.ExpressionValue(),)
    ) -> Mapping[str, sql.DataFrame]:
        return {"expression_value_df": self._create_dataframe(values, self._value_schema)}

    def test__build__single_row(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()
        inputs = self._arrange_inputs()
        builder = builders.CaseSQLBuilder(config, mock.MagicMock(), database)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_transformed(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()

        values = (
            models.ExpressionValue(case_id="case-0", submitter_id="sub-case-0", uqfpkm=34.5),
            models.ExpressionValue(case_id="case-0", submitter_id="sub-case-0", uqfpkm=4.8),
            models.ExpressionValue(case_id="case-1", submitter_id="sub-case-1", uqfpkm=0.4),
        )
        inputs = self._arrange_inputs(values=values)
        builder = builders.CaseSQLBuilder(config, mock.MagicMock(), database)

        result_df = builder.build(**inputs)
        result_rows = frozenset(tuple(r) for r in result_df.collect())

        assert result_rows == frozenset((("case-0", "sub-case-0"), ("case-1", "sub-case-1")))

    def test__build__data_written(
        self,
    ) -> None:
        config = self._arrange_config()
        database = self._arrange_database()
        inputs = self._arrange_inputs()
        builder = builders.CaseSQLBuilder(config, mock.MagicMock(), database)

        _ = builder.build(**inputs)

        database.write.assert_called_once()
