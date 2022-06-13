import dataclasses
from os import path
from typing import Dict, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.gene_expression import builders
from tests.unit import utils


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
def schema_dir(data_dir: str) -> str:
    return path.join(data_dir, "schemas", "builders", "gene_expression", "case")


@pytest.fixture(scope="class")
def primary_aliquot_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "input_primary_aliquot.json")


@pytest.fixture(scope="class")
def final_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "final_case.json")


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

    def arrange_inputs(
        self,
        primary_aliquots: Tuple[PrimaryAliquot, ...] = (PrimaryAliquot(),),
    ) -> Dict[str, sql.DataFrame]:
        primary_aliquot_df = self.spark_session.createDataFrame(
            primary_aliquots, schema=self.primary_aliquot_schema
        )

        return {
            "gene_expression_primary_aliquot_df": primary_aliquot_df,
        }

    def test__build_from_scratch__single_row(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs()
        builder = builders.CaseBuilder(config, sql_context)

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build_from_scratch__data_transformed(self) -> None:
        demographic = Demographic()
        diagnosis = Diagnosis()
        primary_aliquot = PrimaryAliquot(
            demographic=demographic, diagnoses=(diagnosis,)
        )

        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs((primary_aliquot,))
        builder = builders.CaseBuilder(config, sql_context)

        result_df = builder.build_from_scratch(**inputs)
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
