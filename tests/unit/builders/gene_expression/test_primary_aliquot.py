import unittest
from collections.abc import Iterable
from unittest import mock

import more_itertools
from pyspark import sql

from mutation_indexer import es_utils
from mutation_indexer.builders import gene_expression
from mutation_indexer.configuration.builders import gene_expression as ge_config
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


class TestPrimaryAliquotBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._input_file_schema = (
            schemas.GeneExpression.Builders.PrimaryAliquot.FILE.load()
        )
        cls._final_schema = schemas.GeneExpression.Builders.PrimaryAliquot.FINAL.load()

    def arrange_config(self) -> ge_config.Builder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=ge_config.Builder, projects=(), is_cached=False, backup=backup
        )

    def arrange_es_dataframe_util(
        self, data: Iterable[models.File]
    ) -> es_utils.DataFrameUtil:
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)

        dataframe_util.read.return_value = utils.create_dataframe(
            data, schema=self._input_file_schema
        )

        return dataframe_util

    def arrange_builder(
        self, data: tuple[models.File, ...] = (models.File(),)
    ) -> gene_expression.PrimaryAliquotBuilder:
        config = self.arrange_config()
        util = self.arrange_es_dataframe_util(data)
        spark_session = mock.MagicMock(spec=sql.SparkSession)

        return gene_expression.PrimaryAliquotBuilder(config, spark_session, util)

    def test__build__single_row(self) -> None:
        builder = self.arrange_builder()

        result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_translated(self) -> None:
        es_file = models.File()
        es_case = es_file.cases[0]
        builder = self.arrange_builder((es_file,))

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == es_file.file_id
        assert result_row.case_id == es_case.case_id
        assert result_row.submitter_id == es_case.submitter_id
