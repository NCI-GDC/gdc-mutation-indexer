import contextlib
import dataclasses
from collections.abc import Iterable, Iterator
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders import civic
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class CIVICDatum:
    civic_var_id: int = 4
    civic_gene_id: int = 2
    source: str = "gDNA"
    chromosome: str = "chr14"
    start_position: int = 104780214
    reference_allele: str = "C"
    alternative_allele: str = "T"


@pytest.fixture(scope="class")
def input_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.DNA.INPUT.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.DNA.FINAL.load()


class TestDNABuilder:
    @pytest.fixture(autouse=True)
    def load_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        input_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._input_schema = input_schema
        self._final_schema = final_schema

    def arrange_spark_session(
        self, data: Iterable[CIVICDatum] = (CIVICDatum(),)
    ) -> sql.SparkSession:
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.csv.return_value = self._create_dataframe(
            data, self._input_schema
        )

        return spark_session

    def arrange_config(self) -> viz.ResourceBuilder:
        config = mock.MagicMock(
            spec=viz.ResourceBuilder,
            is_cached=False,
            package="",
            resource="",
            schema="",
            backup=mock.MagicMock(path="", mode=build.BackupMode.NEITHER),
        )

        return config

    @contextlib.contextmanager
    def arrange_schemas(self) -> Iterator[None]:
        with mock.patch(
            "mutation_indexer.schemas.load_schema", return_value=self._input_schema
        ):
            yield None

    @contextlib.contextmanager
    def arrange_resources(self) -> Iterator[None]:
        with mock.patch.multiple(
            "importlib.resources", files=mock.DEFAULT, as_file=mock.DEFAULT
        ):
            yield None

    def test__build__data_translated(self) -> None:
        datum = CIVICDatum()
        config = self.arrange_config()
        spark_session = self.arrange_spark_session((datum,))
        builder = civic.DNABuilder(config, spark_session)

        with self.arrange_resources(), self.arrange_schemas():
            result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.chromosome == datum.chromosome
        assert result_row.civic_gene_id == str(datum.civic_gene_id)
        assert result_row.civic_variant_id == str(datum.civic_var_id)
        assert result_row.reference_allele == datum.reference_allele
        assert result_row.start_position == datum.start_position
        assert result_row.tumor_allele == datum.alternative_allele
