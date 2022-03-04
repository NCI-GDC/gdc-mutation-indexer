import unittest

import pytest
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from tests.integration import config


class TestPrimaryAliquotBuilder(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def import_fixtures(self, sqlContext: sql.SQLContext, files_with_linked_cases):
        self.sql_context = sqlContext

    def test__get_case_file_metadata__tcga_kich(self):
        conf = config.TestConfig()
        conf.projects = ["TCGA-KICH"]
        es_dataframe_util = es_utils.DataFrameUtil(conf, self.sql_context)
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            conf, self.sql_context, conf.indexd, es_dataframe_util
        )

        df = primary_aliquot_builder.build()

        files = {
            row.case_id: row.file_id
            for row in df.where(F.col("entity") == F.lit("case")).collect()
        }

        self.assertEqual(len(files), 2)
        self.assertEqual(
            files.get("452135f2-6de6-4593-a091-ddf6344ee431"),
            "acc6c688-a233-46bf-b2d9-7bfec28241ed",
        )
        self.assertEqual(
            files.get("872092b3-d31e-44d7-bd03-e29f52f8ab5a"),
            "cf3708a2-28e1-49cc-9e67-4416b3bf5b1a",
        )

    def test__get_case_file_metadata__tcga(self):
        conf = config.TestConfig()
        conf.projects = ["TCGA"]
        es_dataframe_util = es_utils.DataFrameUtil(conf, self.sql_context)
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            conf, self.sql_context, conf.indexd, es_dataframe_util
        )

        df = primary_aliquot_builder.build()

        files = {
            row.case_id: row.file_id
            for row in df.where(F.col("entity") == F.lit("case")).collect()
        }

        self.assertEqual(len(files), 1)
        self.assertEqual(
            files.get("c65d7c98-9678-401b-9f4d-0e1e83be3697"),
            "213fd6e3-9462-492f-8917-56967be8fc9d",
        )
