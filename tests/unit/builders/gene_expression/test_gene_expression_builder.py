import dataclasses
from os import path
from typing import Dict, Tuple
from unittest import mock

import pytest
from exports import builders
from pyspark import sql
from pyspark.sql import types

from tests.unit import utils


@dataclasses.dataclass(frozen=True)
class Gene:
    gene_id: str = "gene-0"
    expression_value: float = 328382.4458
    symbol: str = "genSym"


@dataclasses.dataclass(frozen=True)
class File:
    file_id: str = "file-0"
    genes: Tuple[Gene, ...] = (Gene(),)


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: str = "case-0"
    days_to_death: int = 38
    ethnicity: str = "hispanic"
    gender: str = "male"
    race: str = "indigenous"
    vital_status: str = "status"
    submitter_id: str = "sub-id"
    project_id: str = "GDC-TEST"
    file_id: str = "file-0"
    age_at_diagnosis: Tuple[int, ...] = (12,)


@pytest.fixture(scope="class")
def schema_dir(data_dir: str) -> str:
    return path.join(
        data_dir, "schemas", "builders", "gene_expression", "gene_expression"
    )


@pytest.fixture(scope="class")
def values_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "input_value.json")


@pytest.fixture(scope="class")
def cases_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "input_case.json")


@pytest.fixture(scope="class")
def final_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "final_gene_expression.json")


class TestGeneExpressionBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        values_schema: types.StructType,
        cases_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.values_schema = values_schema
        self.cases_schema = cases_schema
        self.final_schema = final_schema

    def arrange_inputs(
        self, values: Tuple[File, ...] = (File(),), cases: Tuple[Case, ...] = (Case(),)
    ) -> Dict[str, sql.DataFrame]:
        value_df = self.spark_session.createDataFrame(values, schema=self.values_schema)
        case_df = self.spark_session.createDataFrame(cases, schema=self.cases_schema)

        return {"ge_values_df": value_df, "case_df": case_df}

    def test__build__single_row(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionBuilder(config, sql_context)

        builder.build(**inputs)

        result_df = builder.gene_expression

        assert result_df
        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__no_matching_file_ids(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        inputs = self.arrange_inputs(
            (File(file_id="file-1"),), (Case(file_id="file-2"),)
        )
        builder = builders.GeneExpressionBuilder(config, sql_context)

        builder.build(**inputs)

        result_df = builder.gene_expression

        assert result_df
        assert result_df.count() == 0
        assert result_df.schema == self.final_schema
