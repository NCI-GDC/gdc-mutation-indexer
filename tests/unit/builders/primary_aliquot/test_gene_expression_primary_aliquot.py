import dataclasses
import datetime
from typing import Optional, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from tests.unit.data.schemas.builders import gene_expression as schemas


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
    return schemas.Input.PRIMARY_ALIQUOT_FILE.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Final.PRIMARY_ALIQUOT.load()


class TestGeneExpressionPrimaryAliquotBuilder:
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

    def arrange_config(
        self, projects: Optional[Tuple[str, ...]] = None
    ) -> mock.MagicMock:
        return mock.MagicMock(projects=projects)

    def arrange_es_dataframe_util(
        self, data: Tuple[ESFile, ...] = (ESFile(),)
    ) -> mock.MagicMock:
        dataframe_util = mock.MagicMock()

        dataframe_util.get_dataframe.return_value = self.spark_session.createDataFrame(
            data,  # type: ignore
            schema=self.input_file_schema,
        )

        return dataframe_util

    def test__build_from_scratch__single_row(self) -> None:
        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_es_dataframe_util()
        builder = builders.GeneExpressionPrimaryAliquotBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch()

        assert result_df.schema == self.final_schema
        assert result_df.count() == 1

    def test__build_from_scratch__data_translated(self) -> None:
        config = self.arrange_config()
        sql_context = mock.MagicMock()
        es_file = ESFile()
        es_case = es_file.cases[0]
        dataframe_util = self.arrange_es_dataframe_util((ESFile(),))
        builder = builders.GeneExpressionPrimaryAliquotBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch()
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
