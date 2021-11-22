import functools
import json
from typing import Container, Iterable
from unittest import mock

import pytest

from exports import builders
from pyspark import sql
from pyspark.sql import types

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
        "66d974b9-d64c-4585-a94b-8325e0e11b33",
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
        "e1d964ae-7676-40be-a2c4-b3a13804e1f5",
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
        "c5435e5e-516a-4db6-b761-ef4f5307c15c",
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
        "d519256b-6419-4928-bc17-a7aa32d22450",
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
        "c3c9b822-1a01-4298-b5c5-b1d105c484e9",
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
        "945e4d87-aea7-44cc-bc8d-3088ee5c9910",
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
        "c17be6a4-d22a-4806-97b7-aafae0da4fd2",
        "ENSG00000186092.4",
        "OR4F5",
        "chr1",
        69091,
        70008,
        2,
        2,
        2,
    ),
    (
        "15a00a97-c304-4826-ae3c-c8dddcbac8a6",
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


class TestAscatBuilder:
    ascat_files = ()  # type: Iterable[dict]

    @pytest.fixture(autouse=True)
    def load_fixtures(self, sqlContext: sql.SQLContext) -> None:
        self.sql_context = sqlContext

    @classmethod
    def _load_ascat_files(cls) -> Iterable[dict]:
        with open("tests/unit/data/input/test_ascat_builder_common.ndjson", "r") as f:
            for line in f.readlines():
                yield json.loads(line)

    @classmethod
    def setup_class(cls):
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
            assert project_id is not None, "Invalid test data. Project ID cannot be None."

            if project_id in projects:
                return True

        return False

    def _get_file_df_schema(self) -> types.StructType:
        with open("tests/unit/data/schemas/builders/ascat/es_file_schema.json", "r") as f:
            schema = json.load(f)

            return types.StructType.fromJson(schema)

    def _get_files_df(self, projects: Container[str]) -> sql.DataFrame:
        files = filter(functools.partial(self._file_filter, projects), self.ascat_files)
        rows = tuple((file["file_id"], file["cases"]) for file in files)

        df = self.sql_context.createDataFrame(rows, self._get_file_df_schema())

        return df

    def test__build__tcga_hnsc(self):
        primary_aliquot_data = frozenset(
            (
                ("1805d249-bc24-4eed-b191-86d333d7563f", "fcf1dd86-0f1b-4f3d-a6c6-dcd80e201f20"),
                ("66d974b9-d64c-4585-a94b-8325e0e11b33", "77470620-3cc3-4f23-a666-156e9cd1c81e"),
                ("c5435e5e-516a-4db6-b761-ef4f5307c15c", "5d9d664e-c0d5-4ec4-8cc3-65c518b3ec87"),
                ("9ea1b305-8b2d-43a4-95cf-42bef54126e1", "0e867d2c-937c-4705-afec-c5aef316c2ba"),
                ("c3c9b822-1a01-4298-b5c5-b1d105c484e9", "dfe50668-4203-4ea4-9239-2cc8e3d87de8"),
                ("c17be6a4-d22a-4806-97b7-aafae0da4fd2", "03884466-45fc-4dfb-8108-f32c4315bca8"),
            )
        )
        config = mock.MagicMock()
        config.projects = ["TCGA-HNSC"]
        config.ascat_backup = "neither"
        es_dataframe_util = mock.MagicMock()
        es_dataframe_util.get_dataframe.return_value = self._get_files_df(
            config.projects
        )
        document_service = mock.MagicMock()
        document_service.get_dataframe.return_value = self._get_ascat_document_df()
        builder = builders.AscatBuilder(
            config,
            self.sql_context,
            document_service,
            es_dataframe_util,
        )
        primary_aliquot_df = self.sql_context.createDataFrame(
            tuple((file_id, aliquot_id, "file") for file_id, aliquot_id in primary_aliquot_data),
            ("file_id", "aliquot_id", "entity"),
        )

        ascat_df = builder.build(primary_aliquot_df=primary_aliquot_df)

        actual_aliquot_ids = frozenset(
            (row.file_id, row.aliquot_id) for row in ascat_df.select("aliquot_id", "file_id").collect()
        )
        
        assert actual_aliquot_ids == primary_aliquot_data
