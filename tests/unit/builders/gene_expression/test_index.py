from collections.abc import Iterable
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders.gene_expression import index
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def expression_value_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Index.FINAL.load()


class TestGeneExpressionBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        expression_value_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.expression_value_schema = expression_value_schema
        self.final_schema = final_schema

    def _arrange_inputs(
        self, values: Iterable[models.ExpressionValue] = (models.ExpressionValue(),)
    ) -> dict[str, sql.DataFrame]:
        expression_value_df = self.create_dataframe(
            values, self.expression_value_schema
        )

        return dict(expression_value_df=expression_value_df)

    def _arrange_config(self) -> gene_expression.GeneExpressionIndexBuilder:
        config = mock.MagicMock(
            spec=gene_expression.GeneExpressionIndexBuilder,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            partition_size=1,
            id_field="case_id",
        )

        return config

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs()
        builder = index.IndexBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        value = models.ExpressionValue()
        inputs = self._arrange_inputs((value,))
        builder = index.IndexBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.case_id == value.case_id
        assert result_row.gene_expression_id == value.gene_expression_id
        assert result_row.gene_id == value.gene_id
        assert result_row.submitter_id == value.submitter_id
        assert result_row.symbol == value.symbol
        utils.assert_float_equal(result_row.uqfpkm, value.uqfpkm)
        utils.assert_float_equal(result_row.log2_uqfpkm, value.log2_uqfpkm)
