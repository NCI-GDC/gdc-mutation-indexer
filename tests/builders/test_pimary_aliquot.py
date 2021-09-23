import unittest

import pytest

from exports import builders
from pyspark import sql
from tests_config import TestConfig


class TestPrimaryAliquotBuilder(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def import_fixtures(self, spark_session: sql.SparkSession, files_with_linked_cases):
        self.spark_session = spark_session

    def test__get_case_file_metadata__tcga_kich(self):
        config = TestConfig()
        config.projects = ["TCGA-KICH"]
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            config,
            self.spark_session
        )

        df = primary_aliquot_builder.build_primary_aliquots_for_project()

        files = {row.case_id: row.file_id for row in df.collect()}

        self.assertEquals(len(files), 2)
        self.assertEquals(
            files.get("452135f2-6de6-4593-a091-ddf6344ee431"),
            "acc6c688-a233-46bf-b2d9-7bfec28241ed"
        )
        self.assertEquals(
            files.get("872092b3-d31e-44d7-bd03-e29f52f8ab5a"),
            "cf3708a2-28e1-49cc-9e67-4416b3bf5b1a"
        )

    def test__get_case_file_metadata__tcga(self):
        config = TestConfig()
        config.projects = ["TCGA"]
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            config,
            self.spark_session
        )

        df = primary_aliquot_builder.build_primary_aliquots_for_project()

        files = {row.case_id: row.file_id for row in df.collect()}

        self.assertEquals(len(files), 1)
        self.assertEquals(
            files.get("c65d7c98-9678-401b-9f4d-0e1e83be3697"),
            "213fd6e3-9462-492f-8917-56967be8fc9d"
        )
