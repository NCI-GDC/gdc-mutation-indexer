import dataclasses
import datetime
from typing import Iterable, Optional, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from exports import builders, es_utils
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


@pytest.fixture(scope="class")
def input_file_schema() -> types.StructType:
    return schemas.load_schema("builders/primary_aliquot/input_file.json")


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.load_schema("builders/primary_aliquot/final_primary_aliquot.json")


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

    def _arrange_es_rdd_util(self, files: Iterable[ESFile]) -> es_utils.RDDUtil:
        spark_context = self.spark_session.sparkContext
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
        file_df = self.spark_session.createDataFrame(
            files, schema=self.input_file_schema
        )

        dataframe_util.get_dataframe.return_value = file_df

        return dataframe_util

    def _arrange_builder(
        self,
        es_files: Tuple[ESFile, ...],
        aliquot_data: Optional[Tuple[ESFile, ...]] = None,
    ) -> builders.PrimaryAliquotBuilder:
        aliquot_data = es_files if aliquot_data is None else aliquot_data
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        dataframe_util = self._arrange_es_dataframe_util(es_files)
        rdd_util = self._arrange_es_rdd_util(aliquot_data)

        return builders.PrimaryAliquotBuilder(
            config, sql_context, dataframe_util, rdd_util
        )

    @pytest.mark.parametrize(
        ("files", "aliquot_data"),
        (((ESFile(),), (ESFile(),)), ((ESFile(),), ())),
        ids=("aliquot_exists", "no_aliquots"),
    )
    def test__build_from_scratch__positive_joins(
        self, files: Iterable[ESFile], aliquot_data: Iterable[ESFile]
    ) -> None:
        builder = self._arrange_builder(files, aliquot_data)

        result_df = builder.build()

        assert result_df.count() == 2
        assert result_df.schema == self.final_schema

    @pytest.mark.parametrize(
        ("primay_sample_type", "other_sample_type"),
        (
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
        ),
    )
    def test__build_from_scratch__sample_type_selection(
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

        result_df = builder.build_from_scratch()
        result_case_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )
        result_file_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("file")).collect()
        )

        assert result_case_row.aliquot_id == "a-1"
        assert result_file_row.aliquot_id == "a-1"

    @pytest.mark.parametrize(
        ("primary_datetime", "other_datetime"),
        (
            (
                datetime.datetime.max - datetime.timedelta(microseconds=1),
                datetime.datetime.max,
            ),
            (
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
        ),
        ids=("microsecond_diff", "timezone_diff"),
    )
    def test__build_from_scratch__file_created_datetime(
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

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-1"

    def test__build_from_scratch__file_id(self) -> None:
        files = (ESFile(file_id="f-1"), ESFile(file_id="f-0"))
        builder = self._arrange_builder(files, ())

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-0"

    def test__build_from_scratch__aliquot_none(self) -> None:
        builder = self._arrange_builder((ESFile(),), ())

        result_df = builder.build_from_scratch()
        result_rows = result_df.collect()

        assert all(row.aliquot_id is None for row in result_rows)

    @pytest.mark.parametrize(
        ("primary_datetime", "other_datetime"),
        (
            (
                datetime.datetime.max - datetime.timedelta(microseconds=1),
                datetime.datetime.max,
            ),
            (
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
        ),
        ids=("microsecond_diff", "timezone_diff"),
    )
    def test__build_from_scratch__aliquot_created_datetime(
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

        result_df = builder.build_from_scratch()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-1" for row in result_rows)

    def test__build_from_scratch__aliquot_id(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-0" for row in result_rows)

    def test__build_from_scratch__missing_analytes(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(
                ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),
                ESPortion(analytes=None),
            )
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()

        assert result_df.count() == 2
