import io
from collections.abc import Callable, Iterable
from typing import IO, Unpack
from unittest import mock

import mypy_boto3_s3 as s3
import numpy
import pytest
from pyspark import sql
from pyspark.sql import types
from test_case import UploadKwargs

from mutation_indexer.constants import build
from mutation_indexer.gene_expression import builders, configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def expression_value_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


@pytest.fixture(scope="class")
def uqfpkm_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Binary.FINAL.load()


class TestUQFPKMBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        expression_value_schema: types.StructType,
        uqfpkm_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._expression_value_schema = expression_value_schema
        self._final_schema = uqfpkm_schema

    def _arrange_config(self) -> configuration.BinaryBuilder:
        return mock.MagicMock(
            spec=configuration.BinaryBuilder,
            is_cached=False,
            bucket="bucket",
            log2_uqfpkm_key="log2_{gene_id}.bin",
            uqfpkm_key="{gene_id}.bin",
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def _arrange_s3_client(
        self, validate_upload: Callable[..., None] | None = None
    ) -> mock.MagicMock:
        util = mock.MagicMock()

        if validate_upload:
            util.upload_fileobj.side_effect = validate_upload
        else:
            util.upload_fileobj.return_value = None

        return util

    def _arrange_inputs(
        self, values: Iterable[models.ExpressionValue] = (models.ExpressionValue(),)
    ) -> dict[str, sql.DataFrame]:
        return {
            "expression_value_df": self._create_dataframe(
                values, self._expression_value_schema
            )
        }

    def _arrange_builder(
        self, config: configuration.BinaryBuilder, s3_client: s3.Client
    ) -> builders.BinaryBuilder:
        return builders.BinaryBuilder(config, mock.MagicMock(), s3_client)

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        s3_client = self._arrange_s3_client()
        inputs = self._arrange_inputs()
        builder = self._arrange_builder(config, s3_client)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_transformed(self) -> None:
        values = (
            models.ExpressionValue(
                case_id="case-0", gene_id="gene-1", log2_uqfpkm=40.0, uqfpkm=1.0
            ),
            models.ExpressionValue(
                case_id="case-1", gene_id="gene-0", log2_uqfpkm=30.0, uqfpkm=2.0
            ),
            models.ExpressionValue(
                case_id="case-0", gene_id="gene-0", log2_uqfpkm=20.0, uqfpkm=3.0
            ),
            models.ExpressionValue(
                case_id="case-1", gene_id="gene-1", log2_uqfpkm=10.0, uqfpkm=4.0
            ),
        )
        expected_values = {
            "gene-0": ([20.0, 30.0], [3.0, 2.0]),
            "gene-1": ([40.0, 10.0], [1.0, 4.0]),
        }

        config = self._arrange_config()
        s3_client = self._arrange_s3_client()

        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_client)

        result_df = builder.build(**inputs)
        result_data = {r.gene_id: (r.log2_uqfpkm, r.uqfpkm) for r in result_df.collect()}

        assert result_data == expected_values

    def test__build__s3_upload_called(self) -> None:
        values = (
            models.ExpressionValue(
                case_id="case-0", gene_id="gene-1", log2_uqfpkm=40.0, uqfpkm=1.0
            ),
            models.ExpressionValue(
                case_id="case-1", gene_id="gene-0", log2_uqfpkm=30.0, uqfpkm=2.0
            ),
            models.ExpressionValue(
                case_id="case-0", gene_id="gene-0", log2_uqfpkm=20.0, uqfpkm=3.0
            ),
            models.ExpressionValue(
                case_id="case-1", gene_id="gene-1", log2_uqfpkm=10.0, uqfpkm=4.0
            ),
        )
        config = self._arrange_config()

        expected_values = {
            config.log2_uqfpkm_key.format(gene_id="gene-0"): (20.0, 30.0),
            config.uqfpkm_key.format(gene_id="gene-0"): (3.0, 2.0),
            config.log2_uqfpkm_key.format(gene_id="gene-1"): (40.0, 10.0),
            config.uqfpkm_key.format(gene_id="gene-1"): (1.0, 4.0),
        }

        def validate_upload(data: IO[bytes], **kwargs: Unpack[UploadKwargs]) -> None:
            assert kwargs["Bucket"] == config.bucket
            assert kwargs["Key"] in expected_values
            assert isinstance(data, io.BytesIO)
            assert (
                data.read()
                == numpy.array(expected_values[kwargs["Key"]], dtype=numpy.float32).tobytes()
            )

        s3_client = self._arrange_s3_client(validate_upload)

        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_client)

        _ = builder.build(**inputs)

        s3_client.upload_fileobj.assert_called()
