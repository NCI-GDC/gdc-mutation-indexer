import collections
import unittest
from os import path
from typing import Dict, Iterable
from unittest import mock

import pytest
import yaml

from exports import builders, es_utils
from pyspark.sql import types
from tests.utils import schema_validation


INPUT_FOLDER_PATH = "input/builders/primary_aliquot"
OUTPUT_FOLDER_PATH = "output/builders/primary_aliquot"


class TestPrimaryAliquotBuilder(unittest.TestCase):
    schema_validator = schema_validation.PysparkSchemaValidator()
    maxDiff = None

    @pytest.fixture(autouse=True)
    def fixture_set_up(self, sqlContext, data_dir):
        self.sql_context = sqlContext
        self.input_dir = path.join(data_dir, INPUT_FOLDER_PATH)
        self.output_dir = path.join(data_dir, OUTPUT_FOLDER_PATH)

    def _build_es_dataframe(self, data: Iterable[dict], is_gene_expression: bool):
        sample_fields = [
            types.StructField("sample_id", types.StringType(), False),
            types.StructField("sample_type", types.StringType(), False),
        ]

        if not is_gene_expression:
            sample_fields.append(
                types.StructField(
                    "portions",
                    types.ArrayType(
                        types.StructType(
                            [
                                types.StructField(
                                    "analytes",
                                    types.ArrayType(
                                        types.StructType(
                                            [
                                                types.StructField(
                                                    "aliquots",
                                                    types.ArrayType(
                                                        types.StructType(
                                                            [
                                                                types.StructField(
                                                                    "aliquot_id",
                                                                    types.StringType(),
                                                                )
                                                            ]
                                                        )
                                                    ),
                                                )
                                            ]
                                        )
                                    ),
                                )
                            ]
                        )
                    ),
                )
            )

        min_case_fields = [
            types.StructField("case_id", types.StringType(), False),
            types.StructField(
                "samples",
                types.ArrayType(types.StructType(sample_fields)),
                False,
            ),
        ]
        extra_case_fields = [
            types.StructField("submitter_id", types.StringType(), False),
            types.StructField(
                "demographic",
                types.StructType(
                    [
                        types.StructField("days_to_death", types.IntegerType(), False),
                        types.StructField("ethnicity", types.StringType(), False),
                        types.StructField("gender", types.StringType(), False),
                        types.StructField("race", types.StringType(), False),
                        types.StructField("vital_status", types.StringType(), False),
                    ]
                ),
                False,
            ),
            types.StructField(
                "project",
                types.StructType(
                    [
                        types.StructField("project_id", types.StringType(), False),
                    ]
                ),
                False,
            ),
            types.StructField(
                "diagnoses",
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField(
                                "age_at_diagnosis", types.IntegerType(), False
                            )
                        ]
                    )
                ),
                False,
            ),
        ]
        case_fields = (
            min_case_fields + extra_case_fields
            if is_gene_expression
            else min_case_fields
        )
        schema = types.StructType(
            [
                types.StructField("file_id", types.StringType(), False),
                types.StructField("experimental_strategy", types.StringType(), False),
                types.StructField("created_datetime", types.StringType(), False),
                types.StructField(
                    "cases", types.ArrayType(types.StructType(case_fields)), False
                ),
            ]
        )

        return self.sql_context.createDataFrame(data, schema)

    def _load_data_from_file(self, filename: str, is_input: bool = False):
        data_dir = self.input_dir if is_input else self.output_dir

        with open(path.join(data_dir, filename)) as f:
            return yaml.safe_load(f)

    def _load_data_into_df(self, filename: str, is_gene_expression: bool = False):
        data = self._load_data_from_file(filename, is_input=True)

        return self._build_es_dataframe(data, is_gene_expression)

    def test__build_primary_aliquots_for_project(self):
        # Arrange
        config = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        es_df = self._load_data_into_df("test_primry_aliquot_builder_common.yaml")
        es_dataframe_util.get_dataframe.return_value = es_df
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            config,
            self.sql_context,
            config.indexd,
            es_dataframe_util,
        )

        config.projects = ["TEST0"]

        # Load Expected Results
        expected = self._load_data_from_file(
            "test_build_primary_aliquots_for_project.yaml"
        )
        expected_es_include_fields = expected["expected_es_include_fields"]
        expected_es_query = expected["expected_es_query"]
        expected_data = frozenset(tuple(row) for row in expected["expected_data"])
        expected_schema = schema_validation.Schema(expected["expected_schema"])

        # Act
        result_df = primary_aliquot_builder.build()

        # Assert
        # Check External Calls
        es_dataframe_util.get_dataframe.assert_called_once_with(
            es_utils.Index.File,
            include_fields=expected_es_include_fields,
            query=expected_es_query,
        )

        # Check Result Data
        self.schema_validator.validate_schema(result_df.schema, expected_schema)

        result_collected = result_df.collect()
        result_data = frozenset(
            (
                row.entity,
                row.entity_id,
                row.case_id,
                row.file_id,
                row.experimental_strategy,
                row.aliquot_id,
            )
            for row in result_collected
        )

        self.assertEqual(len(result_collected), len(expected_data))
        self.assertSetEqual(result_data, expected_data)

    @staticmethod
    def _mock_bulk_request(bids: Iterable[str]):
        docs = collections.defaultdict(
            mock.MagicMock
        )  # type: Dict[str, mock.MagicMock]
        docs["file-2"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
            (
                "s3://cleversafe.service.consul/good_url",
                {"type": "cleversafe", "state": "validated"},
            ),
        )
        docs["file-2"].did = "file-2"
        docs["file-4"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            (
                "s3://cleversafe.service.consul/good_url",
                {"type": "cleversafe", "state": "validated"},
            ),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-4"].did = "file-4"
        docs["file-8"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-8"].did = "file-8"

        return (doc for file_id, doc in docs.items() if file_id in bids)

    def test__build_gene_expression_prinary_aliquot_data(self):
        # Arrange
        config = mock.MagicMock()
        config.projects = ["TEST0", "TEST1"]
        config.indexd.bulk_request.side_effect = self._mock_bulk_request
        es_dataframe_util = mock.MagicMock()
        es_df = self._load_data_into_df(
            "test_primry_aliquot_builder_common.yaml", True
        )
        es_dataframe_util.get_dataframe.return_value = es_df
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            config,
            self.sql_context,
            config.indexd,
            es_dataframe_util,
        )

        primary_aliquot_builder.logger = mock.MagicMock()
        primary_aliquot_builder.FILE_URL_BATCH_SIZE = 3

        # Load Expected Results
        expected = self._load_data_from_file(
            "test_build_gene_expression_prinary_aliquot_data.yaml"
        )
        expected_es_include_fields = frozenset(expected["expected_es_include_fields"])
        expected_es_query = expected["expected_es_query"]
        expected_data = {row["case_id"]: row for row in expected["expected_data"]}
        expected_schema = schema_validation.Schema(expected["expected_schema"])

        # Act
        result = primary_aliquot_builder.build_gene_expression_primary_aliquot_data(
            ["type0", "type1"]
        )
        result_df = result.primary_aliquot_df
        result_urls = result.file_urls

        # Assert
        # Check External Calls
        es_dataframe_util.get_dataframe.assert_called_once_with(
            es_utils.Index.File,
            include_fields=expected_es_include_fields,
            query=expected_es_query,
        )
        self.assertEqual(config.indexd.bulk_request.call_count, 2)
        primary_aliquot_builder.logger.warning.assert_has_calls(
            [
                mock.call("File is missing: 'file-8'"),
            ],
            any_order=True,
        )

        # Check Results
        self.schema_validator.validate_schema(result_df.schema, expected_schema)

        result_collected = tuple(row.asDict(True) for row in result_df.collect())
        result_data = {row["case_id"]: row for row in result_collected}

        self.assertEqual(len(result_collected), len(expected_data))
        self.assertSetEqual(set(result_data.keys()), set(expected_data.keys()))
        self.assertDictEqual(result_data, expected_data)

        self.assertEqual(len(result_urls), 2)
        self.assertSetEqual(set(result_urls), {"s3a://good_url"})
