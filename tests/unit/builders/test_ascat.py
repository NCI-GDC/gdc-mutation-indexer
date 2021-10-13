import yaml
from os import path
import unittest
from typing import List, NamedTuple, Sequence
from unittest import mock

import pytest

from exports import builders
from pyspark import sql
from pyspark.sql import types, functions as F

from exports.builders import ascat
from tests.utils import schema_validation

INPUT_FOLDER_PATH = "input/builders/ascat"
OUTPUT_FOLDER_PATH = "output/builders/ascat"


Output = NamedTuple("Output", [("expected_schema", schema_validation.Schema), ("expected_data", Sequence[dict])])
RAW_ASCAT_STRUCT = types.StructType(ascat.RAW_ASCAT_STRUCT.fields + [types.StructField("did", types.StringType())])

class TestAscatBuilder(unittest.TestCase):
    schema_validator = schema_validation.PysparkSchemaValidator()
    maxDiff = None

    @pytest.fixture(autouse=True)
    def import_fixtures(self, sqlContext: sql.SQLContext, data_dir: str):
        self.sql_context = sqlContext
        self.input_dir = path.join(data_dir, INPUT_FOLDER_PATH)
        self.output_dir = path.join(data_dir, OUTPUT_FOLDER_PATH)

    def setUp(self) -> None:
        self.file_df = self.sql_context.read.json(
            path.join(self.input_dir, "test_ascat_builder_files_common.ndjson")
        )
        self.gene_model_df = self.sql_context.read.json(
            path.join(self.input_dir, "test_ascat_builder_gene_model_common.ndjson")
        )
        self.ascat_df = self.sql_context.read.csv(
            path.join(self.input_dir, "test_ascat_builder_ascat_common.tsv"),
            sep="\t",
            header=True,
            # schema=RAW_ASCAT_STRUCT,
        ).select(
            "did",
            "gene_id",
            "gene_name",
            "chromosome",
            F.col("start").cast(types.IntegerType()),
            F.col("end").cast(types.IntegerType()),
            F.col("copy_number").cast(types.IntegerType()),
            F.col("min_copy_number").cast(types.IntegerType()),
            F.col("max_copy_number").cast(types.IntegerType()),
        )
        self.primary_aliquot_df = self.sql_context.read.csv(
            path.join(self.input_dir, "test_ascat_builder_primary_aliquot_common.tsv"),
            sep="\t",
            header=True,
        )

    def _load_file_data(self, project_ids: List[str]) -> sql.DataFrame:
        lit_project_ids = tuple(F.lit(project_id) for project_id in project_ids)

        return self.file_df.where(
            F.arrays_overlap("cases.project.project_id", F.array(*lit_project_ids))
        )

    def _load_output(self, output_file_name: str) -> Output:
        with open(path.join(self.output_dir, output_file_name), "rb") as f:
            output = yaml.safe_load(f)

            return Output(expected_schema=schema_validation.Schema(output["expected_schema"]), expected_data=output["expected_data"])

    def test__build__tcga_hnsc(self):
        config = mock.MagicMock()
        config.projects = ["TCGA-HNSC"]
        config.ascat_backup = "neither"
        es_dataframe_util = mock.MagicMock()
        es_dataframe_util.get_dataframe.return_value = self._load_file_data(
            config.projects
        )
        document_service = mock.MagicMock()
        document_service.get_dataframe.return_value = self.ascat_df
        output = self._load_output("ascat_builder.yaml")
        builder = builders.AscatBuilder(
            config,
            self.sql_context,
            document_service,
            es_dataframe_util,
        )

        ascat_df = builder.build(
            primary_aliquot_df=self.primary_aliquot_df, gene_model_df=self.gene_model_df
        )

        ascat_data = tuple(row.asDict() for row in ascat_df.collect())

        self.assertEqual(len(ascat_data), len(output.expected_data))

        for actual_row, expected_row in zip(ascat_data, output.expected_data):
            self.assertDictEqual(actual_row, expected_row)
