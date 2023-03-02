from typing import Any, Callable, Iterable
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports.builders import civic
from exports.configuration.builders import viz
from exports.constants import build
from tests.unit.data import schemas
from tests.unit.data.models.viz.civic.prot import inputs


@pytest.fixture(scope="module")
def input_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.PROT.INPUT.load()


@pytest.fixture(scope="module")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.PROT.FINAL.load()


class TestPROTBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: Callable[[Iterable[Any], types.StructType], sql.DataFrame],
        input_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.input_schema = input_schema
        self.final_schema = final_schema

    def arrange_config(self) -> viz.PROTBuilder:
        config = mock.MagicMock(
            spec=viz.PROTBuilder,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            data_package="",
            data_resource="",
        )

        return config

    def arrange_spark_session(
        self, prot_data: Iterable[inputs.PROT] = (inputs.PROT(),)
    ) -> sql.SparkSession:
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.csv.return_value = self.create_dataframe(
            prot_data, self.input_schema
        )

        return spark_session

    def arrange_inputs(self) -> civic.PROTInputs:
        return {}

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        input_dfs = self.arrange_inputs()
        builder = civic.PROTBuilder(config, spark_session)

        with mock.patch("importlib.resources.path"):
            result_df = builder.build(**input_dfs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        config = self.arrange_config()
        prot = inputs.PROT()
        spark_session = self.arrange_spark_session((prot,))
        input_dfs = self.arrange_inputs()
        builder = civic.PROTBuilder(config, spark_session)

        with mock.patch("importlib.resources.path"):
            result_df = builder.build(**input_dfs)

        result_row = more_itertools.one(result_df.collect())

        assert result_row.hgvsp_short == prot.hgvsp
        assert result_row.name == prot.hugo_symbol
        assert result_row.prot_civic_gene_id == prot.civic_gene_id
        assert result_row.prot_civic_var_id == prot.civic_var_id
