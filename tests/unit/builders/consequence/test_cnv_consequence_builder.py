from typing import Iterable
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports.builders import consequence
from exports.configuration.builders import viz
from exports.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models.viz import ascat


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.CNV.FINAL.load()


class TestCNVConsequenceBuilder:
    @pytest.fixture(autouse=True)
    def setup(
        self,
        create_dataframe: utils.DataFrameCreator,
        ascat_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.ascat_schema = ascat_schema
        self.final_schema = final_schema

    def _arrange_inputs(
        self, ascat_data: Iterable[ascat.ASCAT] = (ascat.ASCAT(),)
    ) -> consequence.CNVConsequenceInputs:
        return {"ascat_df": self.create_dataframe(ascat_data, self.ascat_schema)}

    def _arrange_config(self) -> viz.Builder:
        return mock.MagicMock(
            spec=viz.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def _arrange_spark_session(self) -> sql.SparkSession:
        return mock.MagicMock(spec=sql.SparkSession)

    def _arrange_builder(self) -> consequence.CNVConsequenceBuilder:
        return consequence.CNVConsequenceBuilder(
            self._arrange_config(), self._arrange_spark_session()
        )

    def test__build__final_schema(self) -> None:
        input_dfs = self._arrange_inputs()
        builder = self._arrange_builder()

        result_df = builder.build(**input_dfs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        ascat_data = ascat.ASCAT()
        input_dfs = self._arrange_inputs((ascat_data,))
        builder = self._arrange_builder()

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cnv_id == ascat_data.cnv_id
        assert len(result_row.consequence) == 1
        assert result_row.consequence[0].consequence_id == ascat_data.consequence_id
        assert result_row.consequence[0].gene.biotype == ascat_data.biotype
        assert result_row.consequence[0].gene.gene_id == ascat_data.gene_id
        assert (
            result_row.consequence[0].gene.is_cancer_gene_census
            == ascat_data.is_cancer_gene_census
        )
        assert result_row.consequence[0].gene.symbol == ascat_data.symbol

    def test__build__consequence_grouped_by_cnv_id(self) -> None:
        ascat_data = (
            ascat.ASCAT(cnv_id="cnv-0", gene_id="g-0"),
            ascat.ASCAT(cnv_id="cnv-0", gene_id="g-1"),
            ascat.ASCAT(cnv_id="cnv-1", gene_id="g-0"),
        )
        input_dfs = self._arrange_inputs(ascat_data)
        builder = self._arrange_builder()

        result_df = builder.build(**input_dfs)
        result_rows = {
            r.cnv_id: frozenset(c.gene.gene_id for c in r.consequence)
            for r in result_df.collect()
        }

        assert result_rows.keys() == frozenset({"cnv-0", "cnv-1"})
        assert result_rows["cnv-0"] == frozenset(("g-0", "g-1"))
        assert result_rows["cnv-1"] == frozenset(("g-0",))

    def test__build__consequence_duplicates_dropped(self) -> None:
        ascat_data = (
            ascat.ASCAT(cnv_id="cnv-0", gene_id="g-0"),
            ascat.ASCAT(cnv_id="cnv-0", gene_id="g-0"),
        )
        input_dfs = self._arrange_inputs(ascat_data)
        builder = self._arrange_builder()

        result_df = builder.build(**input_dfs)
        result_rows = {
            r.cnv_id: tuple(c.gene.gene_id for c in r.consequence)
            for r in result_df.collect()
        }

        assert result_rows.keys() == frozenset({"cnv-0"})
        assert result_rows["cnv-0"] == ("g-0",)
