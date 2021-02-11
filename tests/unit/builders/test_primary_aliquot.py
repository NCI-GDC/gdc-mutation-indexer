from collections import defaultdict
from mock import patch, MagicMock, call
from os import path
from unittest import TestCase
import yaml

import pytest

from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType, TimestampType

from exports.builders.primary_aliquot import PrimaryAliquotBuilder

from tests_config import TestConfig
from utils.schema_validation import Schema, PysparkSchemaValidator


class TestPrimaryAliquotBuilder(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.schema_validator = PysparkSchemaValidator()

    @pytest.fixture(autouse=True)
    def fixture_set_up(self, sqlContext, data_dir):
        self.sql_context = sqlContext
        self.data_dir = data_dir

    def _build_es_dataframe(self, data, load_min):
        min_case_fields = [
            StructField("case_id", StringType(), False),
            StructField("samples", ArrayType(
                StructType([
                    StructField("sample_type", StringType(), False)
                ])
            ), False),
        ]
        extra_case_fields = [
            StructField("submitter_id", StringType(), False),
            StructField("demographic", StructType([
                StructField("days_to_death", IntegerType(), False),
                StructField("ethnicity", StringType(), False),
                StructField("gender", StringType(), False),
                StructField("race", StringType(), False),
                StructField("vital_status", StringType(), False),
            ]), False),
            StructField("project", StructType([
                StructField("project_id", StringType(), False),
            ]), False),
            StructField("diagnoses", ArrayType(
                StructType([
                    StructField("age_at_diagnosis", IntegerType(), False)
                ])
            ), False),
        ]
        case_fiels = min_case_fields if load_min else min_case_fields + extra_case_fields
        schema = StructType([
            StructField("file_id", StringType(), False),
            StructField("experimental_strategy", StringType(), False),
            StructField("created_datetime", StringType(), False),
            StructField("cases", ArrayType(StructType(case_fiels)), False)
        ])

        return self.sql_context.createDataFrame(data, schema)

    def _assert_get_dataframe_from_es_called_with(
        self,
        get_dataframe_from_es,
        expected_config,
        expected_include_fields,
        expected_query
    ):
        calls = get_dataframe_from_es.call_args_list

        self.assertEquals(len(calls), 1)

        call = calls[0]
        args = call.args
        kwargs = call.kwargs

        self.assertTupleEqual(
            (self.sql_context, expected_config, expected_config.graph_file_index),
            args
        )
        self.assertSetEqual(kwargs.get("include_fields", {}), expected_include_fields)
        self.assertDictEqual(kwargs.get("query", {}), expected_query)

    def _load_data_from_file(self, filename):
        with open(path.join(self.data_dir, filename)) as f:
            return yaml.safe_load(f)

    def _load_data_into_df(self, filename, load_min=True):
        data = self._load_data_from_file(filename)

        return self._build_es_dataframe(data, load_min)

    @patch("exports.builders.primary_aliquot.get_dataframe_from_es")
    def test__build_primary_aliquots_for_project(self, get_dataframe_from_es):
        # Arrange
        config = MagicMock()
        primary_aliquot_builder = PrimaryAliquotBuilder(self.sql_context, config)
        es_df = self._load_data_into_df("input/test_primry_aliquot_builder_common.yaml")
        get_dataframe_from_es.return_value = es_df

        config.projects = ["TEST0"]

        ## Load Expected Results
        expected = self._load_data_from_file("output/test_build_primary_aliquots_for_project.yaml")
        expected_es_include_fields = frozenset(expected["expected_es_include_fields"])
        expected_es_query = expected["expected_es_query"]
        expected_data = frozenset(tuple(row) for row in expected["expected_data"])
        expected_schema = Schema(expected["expected_schema"])

        # Act
        result_df = primary_aliquot_builder.build_primary_aliquots_for_project()

        # Assert
        ## Check External Calls
        self._assert_get_dataframe_from_es_called_with(
            get_dataframe_from_es,
            config,
            expected_es_include_fields,
            expected_es_query
        )

        ## Check Result Data
        self.schema_validator.validate_schema(result_df.schema, expected_schema)
        
        result_collected = result_df.collect()
        result_data = frozenset(
            (
                row["case_id"], 
                row["file_id"], 
                row["experimental_strategy"],
            ) 
            for row in result_collected
        )

        self.assertEquals(len(result_collected), len(expected_data))
        self.assertSetEqual(result_data, expected_data)

    @staticmethod
    def _mock_bulk_request(bids):
        docs = defaultdict(MagicMock)
        docs["file-2"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
            ("s3://cleversafe.service.consul/good_url", {"type": "cleversafe", "state": "validated"}),
        )
        docs["file-2"].did = "file-2"
        docs["file-4"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("s3://cleversafe.service.consul/good_url", {"type": "cleversafe", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-4"].did = "file-4"
        docs["file-8"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-8"].did = "file-8"

        return (doc for file_id, doc in docs.items() if file_id in bids)
    
    @patch("exports.builders.primary_aliquot.get_dataframe_from_es")
    def test__build_gene_expression_prinary_aliquot_data(self, get_dataframe_from_es):
        # Arrange
        config = MagicMock()
        primary_aliquot_builder = PrimaryAliquotBuilder(self.sql_context, config)
        es_df = self._load_data_into_df("input/test_primry_aliquot_builder_common.yaml", False)
        get_dataframe_from_es.return_value = es_df

        primary_aliquot_builder.logger = MagicMock()
        primary_aliquot_builder.FILE_URL_BATCH_SIZE = 3

        config.projects = ["TEST0", "TEST1"]
        config.indexd.bulk_request.side_effect = self._mock_bulk_request

        ## Load Expected Results
        expected = self._load_data_from_file("output/test_build_gene_expression_prinary_aliquot_data.yaml")
        expected_es_include_fields = frozenset(expected["expected_es_include_fields"])
        expected_es_query = expected["expected_es_query"]
        expected_data = {row["case_id"]: row for row in expected["expected_data"]}
        expected_schema = Schema(expected["expected_schema"])

        # Act
        result = primary_aliquot_builder.build_gene_expression_primary_aliquot_data(["type0", "type1"])
        result_df = result.primary_aliquot_df
        result_urls = result.file_urls

        # Assert
        ## Check External Calls
        self._assert_get_dataframe_from_es_called_with(
            get_dataframe_from_es,
            config,
            expected_es_include_fields,
            expected_es_query
        )
        self.assertEquals(config.indexd.bulk_request.call_count, 2)
        primary_aliquot_builder.logger.warning.assert_has_calls([
            call("File is missing: 'file-8'"),
        ], any_order=True)

        ## Check Results
        self.schema_validator.validate_schema(result_df.schema, expected_schema)
        
        result_collected = tuple(row.asDict(True) for row in result_df.collect())
        result_data = {row["case_id"]: row for row in result_collected}

        self.assertEquals(len(result_collected), len(expected_data))
        self.assertSetEqual(set(result_data.keys()), set(expected_data.keys()))
        self.assertDictEqual(result_data, expected_data)

        self.assertEquals(len(result_urls), 2)
        self.assertSetEqual(set(result_urls), set(("s3a://good_url",)))
