from collections.abc import Iterable
from unittest import mock

import numpy
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import aws
from mutation_indexer.builders.gene_expression import uqfpkm
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def expression_value_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


@pytest.fixture(scope="class")
def uqfpkm_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.UQFPKM.UQFPKM.load()


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

    def _arrange_config(self) -> gene_expression.ValueArrayBuilder:
        return mock.MagicMock(
            spec=gene_expression.ValueArrayBuilder,
            is_cached=False,
            bucket="bucket",
            key_pattern="key_{gene_id}",
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def _arrange_s3_util(self) -> mock.MagicMock:
        util = mock.MagicMock(spec=aws.S3Util)
        util.write.return_value = None

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
        self, config: gene_expression.ValueArrayBuilder, s3_util: aws.S3Util
    ) -> uqfpkm.UQFPKMBuilder:
        return uqfpkm.UQFPKMBuilder(config, mock.MagicMock(), s3_util)

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        s3_util = self._arrange_s3_util()
        inputs = self._arrange_inputs()
        builder = self._arrange_builder(config, s3_util)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_transformed(self) -> None:
        config = self._arrange_config()
        s3_util = self._arrange_s3_util()
        values = (
            models.ExpressionValue(case_id="case-0", gene_id="gene-1", uqfpkm=1.0),
            models.ExpressionValue(case_id="case-1", gene_id="gene-0", uqfpkm=2.0),
            models.ExpressionValue(case_id="case-0", gene_id="gene-0", uqfpkm=3.0),
            models.ExpressionValue(case_id="case-1", gene_id="gene-1", uqfpkm=4.0),
        )
        expected_values = {"gene-0": [3.0, 2.0], "gene-1": [1.0, 4.0]}
        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_util)

        result_df = builder.build(**inputs)
        result_data = {r.gene_id: r.values for r in result_df.collect()}

        assert result_data == expected_values

    def test__build__s3_write_called(self) -> None:
        config = self._arrange_config()
        s3_util = self._arrange_s3_util()
        values = (
            models.ExpressionValue(case_id="case-0", gene_id="gene-1", uqfpkm=1.0),
            models.ExpressionValue(case_id="case-1", gene_id="gene-0", uqfpkm=2.0),
            models.ExpressionValue(case_id="case-0", gene_id="gene-0", uqfpkm=3.0),
            models.ExpressionValue(case_id="case-1", gene_id="gene-1", uqfpkm=4.0),
        )
        inputs = self._arrange_inputs(values)
        builder = self._arrange_builder(config, s3_util)

        _ = builder.build(**inputs)

        expected_values = (("gene-0", (3.0, 2.0)), ("gene-1", (1.0, 4.0)))

        for gene_id, values in expected_values:
            key = config.key_pattern.format(gene_id=gene_id)
            data = numpy.array(values, dtype=numpy.float32).tobytes()

            s3_util.write.assert_any_call(config.bucket, key, data)
