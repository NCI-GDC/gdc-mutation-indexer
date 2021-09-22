import functools
import json
import unittest
from typing import Container, Iterable
from unittest import mock

import pytest

import tests_config
from exports import builders
from pyspark import sql
from pyspark.sql import types

FILE_SCHEMA = types.StructType(
    [
        types.StructField("file_id", types.StringType()),
        types.StructField(
            "cases",
            types.ArrayType(
                types.StructType(
                    [
                        types.StructField("case_id", types.StringType()),
                        types.StructField(
                            "samples",
                            types.ArrayType(
                                types.StructType(
                                    [
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
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)
ASCAT_DOCUMENTS = (
    (
        "1805d249-bc24-4eed-b191-86d333d7563f",
        "ENSG00000223972.5",
        "DDX11L1",
        "chr1",
        11869,
        14409,
        None,
        None,
        None,
    ),
    (
        "5d9d664e-c0d5-4ec4-8cc3-65c518b3ec87",
        "ENSG00000227232.5",
        "WASH7P",
        "chr1",
        14404,
        29570,
        None,
        None,
        None,
    ),
    (
        "66d974b9-d64c-4585-a94b-8325e0e11b33",
        "ENSG00000278267.1",
        "MIR6859-3",
        "chr1",
        17369,
        17436,
        None,
        None,
        None,
    ),
    (
        "66d974b9-d64c-4585-a94b-8325e0e11b33",
        "ENSG00000243485.3",
        "RP11-34P13.3",
        "chr1",
        29554,
        31109,
        None,
        None,
        None,
    ),
    (
        "9ea1b305-8b2d-43a4-95cf-42bef54126e1",
        "ENSG00000274890.1",
        "MIR1302-9",
        "chr1",
        30366,
        30503,
        None,
        None,
        None,
    ),
    (
        "c5435e5e-516a-4db6-b761-ef4f5307c15c",
        "ENSG00000237613.2",
        "FAM138A",
        "chr1",
        34554,
        36081,
        None,
        None,
        None,
    ),
    (
        "c17be6a4-d22a-4806-97b7-aafae0da4fd2",
        "ENSG00000268020.3",
        "OR4G4P",
        "chr1",
        52473,
        53312,
        None,
        None,
        None,
    ),
    (
        "1805d249-bc24-4eed-b191-86d333d7563f",
        "ENSG00000240361.1",
        "OR4G11P",
        "chr1",
        62948,
        63887,
        2,
        2,
        2,
    ),
    (
        "c3c9b822-1a01-4298-b5c5-b1d105c484e9",
        "ENSG00000186092.4",
        "OR4F5",
        "chr1",
        69091,
        70008,
        2,
        2,
        2,
    ),
)


class TestAscatBuilder(unittest.TestCase):
    ascat_files = ()  # type: Iterable[dict]

    @pytest.fixture(autouse=True)
    def import_fixtures(self, sqlContext: sql.SQLContext):
        self.sql_context = sqlContext

    @classmethod
    def _load_ascat_files(cls) -> Iterable[dict]:
        with open("tests/data/input/ascat/files.ndjson", "r") as f:
            for line in f.readlines():
                yield json.loads(line)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.ascat_files = tuple(cls._load_ascat_files())

    def _get_ascat_document_df(self) -> sql.DataFrame:

        return self.sql_context.createDataFrame(
            ASCAT_DOCUMENTS,
            (
                "did",
                "gene_id",
                "gene_name",
                "chromosome",
                "start",
                "end",
                "copy_number",
                "min_copy_number",
                "max_copy_number",
            ),
        )

    def _file_filter(self, projects: Container[str], file: dict) -> bool:
        project_ids = (
            case.get("project", {}).get("project_id") for case in file.get("cases", ())
        )

        for project_id in project_ids:
            self.assertIsNotNone(
                project_id, "Invalid test data. Project ID cannot be None."
            )

            if project_id in projects:
                return True

        return False

    def _get_files_df(self, projects: Container[str]) -> sql.DataFrame:
        files = filter(functools.partial(self._file_filter, projects), self.ascat_files)
        rows = tuple((file["file_id"], file["cases"]) for file in files)

        df = self.sql_context.createDataFrame(rows, FILE_SCHEMA)

        return df

    def test__todo(self):
        primary_aliquot_ids = frozenset(
            (
                "fcf1dd86-0f1b-4f3d-a6c6-dcd80e201f20",
                "77470620-3cc3-4f23-a666-156e9cd1c81e",
                "5d9d664e-c0d5-4ec4-8cc3-65c518b3ec87",
                "0e867d2c-937c-4705-afec-c5aef316c2ba",
                "97b1603a-66aa-4e93-96e9-2f6d714ad31f",
                "bde1e806-1437-46df-b3e2-0350aa50ea8d",
            )
        )
        config = tests_config.TestConfig()
        config.projects = ["TCGA-HNSC"]
        es_dataframe_util = mock.MagicMock()
        es_dataframe_util.get_dataframe.return_value = self._get_files_df(
            config.projects
        )
        document_service = mock.MagicMock()
        document_service.get_dataframe.return_value = self._get_ascat_document_df()
        builder = builders.AscatBuilder(
            document_service,
            es_dataframe_util,
            config,
        )
        primary_aliquot_df = self.sql_context.createDataFrame(
            tuple((aliquot_id,) for aliquot_id in primary_aliquot_ids),
            ("aliquot_id",),
        )

        ascat_df = builder.build(primary_aliquot_df)

        actual_aliquot_ids = frozenset(
            row.aliquot_id for row in ascat_df.select("aliquot_id").collect()
        )
        self.assertSetEqual(actual_aliquot_ids, primary_aliquot_ids)
