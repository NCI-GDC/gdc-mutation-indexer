import dataclasses
from typing import Dict, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.configuration.builders import gene_expression
from exports.constants import build
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class Demographic:
    days_to_death: int = 20
    ethnicity: str = "non-hispanic"
    gender: str = "female"
    race: str = "first nations"
    vital_status: str = "dead"


@dataclasses.dataclass(frozen=True)
class Project:
    project_id: str = "GDC-TEST"


@dataclasses.dataclass(frozen=True)
class Diagnosis:
    age_at_diagnosis: int = 74


@dataclasses.dataclass(frozen=True)
class Sample:
    sample_id: str = "sample-0"
    sample_type: str = "tumor"


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    file_id: str = "file-0"
    case_id: str = "case-0"
    submitter_id: str = "case 0"
    demographic: Demographic = Demographic()
    project: Project = Project()
    diagnoses: Tuple[Diagnosis, ...] = (Diagnosis(),)
    samples: Tuple[Sample, ...] = (Sample(),)


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Case.FINAL.load()


class TestGeneExpressionCaseInputBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        primary_aliquot_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.primary_aliquot_schema = primary_aliquot_schema
        self.final_schema = final_schema

    def arrange_config(self) -> gene_expression.Builder:
        return mock.MagicMock(
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def arrange_inputs(
        self,
        primary_aliquots: Tuple[PrimaryAliquot, ...] = (PrimaryAliquot(),),
    ) -> Dict[str, sql.DataFrame]:
        primary_aliquot_df = self.spark_session.createDataFrame(
            primary_aliquots,  # type: ignore
            schema=self.primary_aliquot_schema,
        )

        return {
            "primary_aliquot_df": primary_aliquot_df,
        }

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionCaseInputBuilder(config, spark_session)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_transformed(self) -> None:
        demographic = Demographic()
        diagnosis = Diagnosis()
        primary_aliquot = PrimaryAliquot(
            demographic=demographic, diagnoses=(diagnosis,)
        )

        config = self.arrange_config()
        spark_session = mock.MagicMock()
        inputs = self.arrange_inputs((primary_aliquot,))
        builder = builders.GeneExpressionCaseInputBuilder(config, spark_session)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.case_id == primary_aliquot.case_id
        assert result_row.days_to_death == demographic.days_to_death
        assert result_row.ethnicity == demographic.ethnicity
        assert result_row.gender == demographic.gender
        assert result_row.race == demographic.race
        assert result_row.vital_status == demographic.vital_status
        assert result_row.submitter_id == primary_aliquot.submitter_id
        assert result_row.project_id == primary_aliquot.project.project_id
        assert result_row.file_id == primary_aliquot.file_id
        assert len(result_row.age_at_diagnosis) == 1
        assert (
            more_itertools.one(result_row.age_at_diagnosis)
            == diagnosis.age_at_diagnosis
        )
