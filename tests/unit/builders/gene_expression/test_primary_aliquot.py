import dataclasses
import datetime
from typing import Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import es_utils
from exports.builders import gene_expression
from exports.configuration.builders import gene_expression as ge_config
from exports.constants import build
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class ESDemographic:
    days_to_death: int = 3
    ethnicity: str = "hispanic"
    gender: str = "male"
    race: str = "mixed"
    vital_status: str = "?"


@dataclasses.dataclass(frozen=True)
class ESDiagnosis:
    age_at_diagnosis: int = 100


@dataclasses.dataclass(frozen=True)
class ESProject:
    project_id: str = "GDC-TEST"


@dataclasses.dataclass(frozen=True)
class ESSample:
    sample_id: str = "s-0"
    sample_type: str = "Primay Tumor"


@dataclasses.dataclass(frozen=True)
class ESCase:
    case_id: str = "c-0"
    submitter_id: str = "case0"
    project: ESProject = ESProject()
    demographic: ESDemographic = ESDemographic()
    samples: Tuple[ESSample, ...] = (ESSample(),)
    diagnoses: Tuple[ESDiagnosis, ...] = (ESDiagnosis(),)


@dataclasses.dataclass(frozen=True)
class ESFile:
    file_id: str = "f-0"
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "WXS"
    cases: Tuple[ESCase, ...] = (ESCase(),)


@pytest.fixture(scope="class")
def input_file_schema() -> types.StructType:
    return schemas.load_schema(
        "builders/gene_expression/primary_aliquot/input_file.json"
    )


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.load_schema("builders/gene_expression/primary_aliquot/final.json")


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

    def arrange_config(self) -> ge_config.Builder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=ge_config.Builder, projects=(), is_cached=False, backup=backup
        )

    def arrange_es_dataframe_util(
        self, data: Tuple[ESFile, ...]
    ) -> es_utils.DataFrameUtil:
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)

        dataframe_util.read.return_value = self.spark_session.createDataFrame(
            data,  # type: ignore
            schema=self.input_file_schema,
        )

        return dataframe_util

    def arrange_builder(
        self, data: Tuple[ESFile, ...] = (ESFile(),)
    ) -> gene_expression.PrimaryAliquotBuilder:
        config = self.arrange_config()
        util = self.arrange_es_dataframe_util(data)
        spark_session = mock.MagicMock(spec=sql.SparkSession)

        return gene_expression.PrimaryAliquotBuilder(config, spark_session, util)

    def test__build__single_row(self) -> None:
        builder = self.arrange_builder()

        result_df = builder.build()

        assert result_df.schema == self.final_schema
        assert result_df.count() == 1

    def test__build__data_translated(self) -> None:
        es_file = ESFile()
        es_case = es_file.cases[0]
        builder = self.arrange_builder((es_file,))

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == es_file.file_id
        assert result_row.case_id == es_case.case_id
        assert result_row.submitter_id == es_case.submitter_id
        assert result_row.demographic.asDict() == dataclasses.asdict(
            es_case.demographic
        )
        assert result_row.project.asDict() == dataclasses.asdict(es_case.project)
        assert len(result_row.samples) == 1
        assert result_row.samples[0].asDict() == dataclasses.asdict(es_case.samples[0])
        assert len(result_row.diagnoses) == 1
        assert result_row.diagnoses[0].asDict() == dataclasses.asdict(
            es_case.diagnoses[0]
        )
