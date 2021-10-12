import unittest
from os import path
from typing import Iterable, NamedTuple
from unittest import mock
import json

import pytest
import yaml

from exports import builders
from pyspark import sql
from tests.utils import schema_validation
from pyspark.sql import functions as F


INPUT_FOLDER_PATH = "input/builders/gene_model"
OUTPUT_FOLDER_PATH = "output/builders/gene_model"
Outcome = NamedTuple(
    "Outcome",
    [("expected_schema", schema_validation.Schema), ("expected_data", Iterable[dict])],
)


class TestGeneModelBuilder(unittest.TestCase):
    schema_validator = schema_validation.PysparkSchemaValidator()
    maxDiff = None

    @pytest.fixture(autouse=True)
    def import_fixtures(self, sqlContext: sql.SQLContext, data_dir: str):
        self.sql_context = sqlContext
        self.input_dir = path.join(data_dir, INPUT_FOLDER_PATH)
        self.output_dir = path.join(data_dir, OUTPUT_FOLDER_PATH)

    def _load_outcome(self, file_path: str) -> Outcome:
        with open(file_path, "r") as f:
            outcome = yaml.safe_load(f)

            return Outcome(
                expected_schema=schema_validation.Schema(outcome["expected_schema"]),
                expected_data=outcome["expected_data"],
            )

    def test__build__basic(self):
        config = mock.MagicMock(
            gene_model_file=path.join(self.input_dir, "gene_model.ndjson"),
            citobands_file=path.join(self.input_dir, "citobans.tsv"),
            census_file=path.join(self.input_dir, "census.tsv"),
            gene_model_backup="neither",
        )
        builder = builders.GeneModelBuilder(config, self.sql_context)

        outcome = self._load_outcome(path.join(self.output_dir, "gene_model_builder.yaml"))

        result_df = builder.build()
        results = tuple(row.asDict(recursive=True) for row in result_df.collect())

        self.schema_validator.validate_schema(result_df.schema, outcome.expected_schema)
        self.assertEqual(len(results), len(outcome.expected_data))

        for result_row, expected_row in zip(results, outcome.expected_data):
            self.assertDictEqual(result_row, expected_row)
