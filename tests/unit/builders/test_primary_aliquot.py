import dataclasses
import datetime
import unittest
from typing import Iterable, Optional, Tuple
from unittest import mock

import more_itertools
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import fixtures, utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class ESAliquot:
    aliquot_id: str = "a-0"
    created_datetime: Optional[str] = datetime.datetime.min.isoformat(
        timespec="microseconds"
    )

    def to_rdd_data(self) -> dict:
        if self.created_datetime:
            return {
                "aliquot_id": self.aliquot_id,
                "created_datetime": self.created_datetime,
            }

        return {"aliquot_id": self.aliquot_id}


@dataclasses.dataclass(frozen=True)
class ESAnalyte:
    aliquots: Tuple[ESAliquot, ...] = (ESAliquot(),)

    def to_rdd_data(self) -> dict:
        return {"aliquots": tuple(aliquot.to_rdd_data() for aliquot in self.aliquots)}


@dataclasses.dataclass(frozen=True)
class ESPortion:
    analytes: Optional[Tuple[ESAnalyte, ...]] = (ESAnalyte(),)

    def to_rdd_data(self) -> dict:
        analytes = (
            None
            if self.analytes is None
            else tuple(analyte.to_rdd_data() for analyte in self.analytes)
        )

        return {"analytes": analytes}


@dataclasses.dataclass(frozen=True)
class ESSample:
    sample_id: str = "s-0"
    sample_type: str = "Primay Tumor"
    portions: Tuple[ESPortion, ...] = (ESPortion(),)

    def to_rdd_data(self) -> dict:
        return {
            "sample_id": self.sample_id,
            "portions": tuple(portion.to_rdd_data() for portion in self.portions),
        }


@dataclasses.dataclass(frozen=True)
class ESCase:
    case_id: str = "c-0"
    samples: Tuple[ESSample, ...] = (ESSample(),)

    def to_rdd_data(self) -> dict:
        return {
            "case_id": self.case_id,
            "samples": tuple(sample.to_rdd_data() for sample in self.samples),
        }


@dataclasses.dataclass(frozen=True)
class ESFile:
    file_id: str = "f-0"
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "WXS"
    cases: Tuple[ESCase, ...] = (ESCase(),)

    def to_rdd_data(self) -> tuple:
        return (
            self.file_id,
            {
                "cases": tuple(case.to_rdd_data() for case in self.cases),
                "file_id": self.file_id,
            },
        )


class TestPrimaryAliquotBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._input_file_schema = schemas.Viz.Builders.PrimaryAliquot.FILE.load()
        cls._final_schema = schemas.Viz.Builders.PrimaryAliquot.FINAL.load()

    def _arrange_es_rdd_util(self, files: Iterable[ESFile]) -> es_utils.RDDUtil:
        spark_context = fixtures.SPARK_SESSION.sparkContext
        rdd_util = mock.MagicMock(spec=es_utils.RDDUtil)

        rdd_util.get_rdd.return_value = spark_context.parallelize(
            file.to_rdd_data() for file in files
        )

        return rdd_util

    def _arrange_es_dataframe_util(
        self,
        files: Iterable[ESFile],
    ) -> es_utils.DataFrameUtil:
        files = files if isinstance(files, tuple) else tuple(files)
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        file_df = utils.create_dataframe(files, self._input_file_schema)

        dataframe_util.read.return_value = file_df

        return dataframe_util

    def _arrange_config(self) -> viz.Builder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=viz.Builder, is_cached=False, backup=backup, projects=()
        )

    def _arrange_builder(
        self,
        es_files: Iterable[ESFile],
        aliquot_data: Optional[Iterable[ESFile]] = None,
    ) -> builders.PrimaryAliquotBuilder:
        aliquot_data = es_files if aliquot_data is None else aliquot_data
        config = self._arrange_config()
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        dataframe_util = self._arrange_es_dataframe_util(es_files)
        rdd_util = self._arrange_es_rdd_util(aliquot_data)

        return builders.PrimaryAliquotBuilder(
            config, spark_session, dataframe_util, rdd_util
        )

    @utils.parametrize[Iterable[ESFile], Iterable[ESFile]](
        aliquot_exists=((ESFile(),), (ESFile(),)),
        ids=((ESFile(),), ()),
    )
    def test__build__positive_joins(
        self, files: Iterable[ESFile], aliquot_data: Iterable[ESFile]
    ) -> None:
        builder = self._arrange_builder(files, aliquot_data)

        result_df = builder.build()

        assert result_df.count() == 2
        assert result_df.schema == self._final_schema

    @utils.parametrize(
        ("Primary Tumor", "Primary Blood Derived Cancer - Bone Marrow"),
        (
            "Primary Blood Derived Cancer - Bone Marrow",
            "Primary Blood Derived Cancer - Peripheral Blood",
        ),
        ("Primary Blood Derived Cancer - Peripheral Blood", "Metastatic"),
        ("Metastatic", "Additional Metastatic"),
        ("Additional Metastatic", "Recurrent Tumor"),
        ("Recurrent Tumor", "Recurrent Blood Derived Cancer - Bone Marrow"),
        (
            "Recurrent Blood Derived Cancer - Bone Marrow",
            "Recurrent Blood Derived Cancer - Peripheral Blood",
        ),
        (
            "Recurrent Blood Derived Cancer - Peripheral Blood",
            "Additional - New Primary",
        ),
        ("Additional - New Primary", "OTHER"),
    )
    def test__build__sample_type_selection(
        self, primay_sample_type: str, other_sample_type: str
    ) -> None:
        other_portions = (
            ESPortion(analytes=(ESAnalyte(aliquots=(ESAliquot(aliquot_id="a-0"),)),)),
        )
        primary_portions = (
            ESPortion(analytes=(ESAnalyte(aliquots=(ESAliquot(aliquot_id="a-1"),)),)),
        )
        samples = (
            ESSample(
                sample_id="s-0",
                sample_type=other_sample_type,
                portions=other_portions,
            ),
            ESSample(
                sample_id="s-1",
                sample_type=primay_sample_type,
                portions=primary_portions,
            ),
        )
        file = ESFile(cases=(ESCase(samples=samples),))
        builder = self._arrange_builder((file,))

        result_df = builder.build()
        result_case_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )
        result_file_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("file")).collect()
        )

        assert result_case_row.aliquot_id == "a-1"
        assert result_file_row.aliquot_id == "a-1"

    @utils.parametrize(
        microsecond_diff=(
            datetime.datetime.max - datetime.timedelta(microseconds=1),
            datetime.datetime.max,
        ),
        timezone_diff=(
            datetime.datetime(
                1970,
                1,
                12,
                8,
                45,
                34,
                203025,
                datetime.timezone(datetime.timedelta(hours=-5)),
            ),
            datetime.datetime(
                1970,
                1,
                12,
                8,
                45,
                34,
                203025,
                datetime.timezone(datetime.timedelta(hours=-6)),
            ),
        ),
    )
    def test__build__file_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        files = (
            ESFile(
                file_id="f-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            ESFile(
                file_id="f-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        builder = self._arrange_builder(files, ())

        result_df = builder.build()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-1"

    def test__build__file_id(self) -> None:
        files = (ESFile(file_id="f-1"), ESFile(file_id="f-0"))
        builder = self._arrange_builder(files, ())

        result_df = builder.build()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-0"

    def test__build__aliquot_none(self) -> None:
        builder = self._arrange_builder((ESFile(),), ())

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id is None for row in result_rows)

    @utils.parametrize(
        microsecond_diff=(
            datetime.datetime.max - datetime.timedelta(microseconds=1),
            datetime.datetime.max,
        ),
        timezone_diff=(
            datetime.datetime(
                1970,
                1,
                12,
                8,
                45,
                34,
                203025,
                datetime.timezone(datetime.timedelta(hours=-5)),
            ),
            datetime.datetime(
                1970,
                1,
                12,
                8,
                45,
                34,
                203025,
                datetime.timezone(datetime.timedelta(hours=-6)),
            ),
        ),
    )
    def test__build__aliquot_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        aliquots = (
            ESAliquot(
                aliquot_id="a-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            ESAliquot(
                aliquot_id="a-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        sample = ESSample(
            portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-1" for row in result_rows)

    def test__build__aliquot_id(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-0" for row in result_rows)

    def test__build__missing_analytes(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(
                ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),
                ESPortion(analytes=None),
            )
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build()

        assert result_df.count() == 2
