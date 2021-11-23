import json
from os import path
from unittest import mock

import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.builders import ascat


def _load_schema(schema_dir: str, file_name: str) -> types.StructType:
    file_name = path.join(schema_dir, file_name)

    with open(file_name, "r") as f:
        return types.StructType.fromJson(json.load(f))


def _arrange_dataframe_util(dataframe: sql.DataFrame) -> mock.MagicMock:
    dataframe_util = mock.MagicMock()

    dataframe_util.get_dataframe.return_value = dataframe

    return dataframe_util


class TestAscatBuilder:
    @property
    def es_file_schema(self) -> types.StructType:
        if not hasattr(self, "_es_file_schema"):
            self._es_file_schema = _load_schema(self.schema_dir, "es_file_schema.json")

        return self._es_file_schema

    @property
    def final_ascat_schema(self) -> types.StructType:
        if not hasattr(self, "_final_ascat_schema"):
            self._final_ascat_schema = _load_schema(
                self.schema_dir, "final_ascat_schema.json"
            )

        return self._final_ascat_schema

    @pytest.fixture(autouse=True)
    def initialize_fixtures(self, sqlContext: sql.SQLContext, data_dir: str) -> None:
        self.sql_context = sqlContext
        self.schema_dir = path.join(data_dir, "schemas", "builders", "ascat")

    @mock.patch("exports.es_utils.iterate_es_results")
    def test__build_from_scratch__single_record(
        self,
        iterate_es_results: mock.MagicMock,
    ) -> None:
        file_row = {
            "file_id": "file-0",
            "cases": [
                {
                    "case_id": "case-0",
                    "samples": [
                        {
                            "portions": [
                                {
                                    "analytes": [
                                        {"aliquots": [{"aliquot_id": "aliquot-0"}]}
                                    ]
                                }
                            ]
                        }
                    ],
                }
            ],
        }
        document_row = ("file-0", "GENE_ID", "symbol", "X", 0, 100, 33, 2, 2)

        primary_aliquot_df = self.sql_context.createDataFrame(
            (("file", "file-0", "aliquot-0"),), ("entity", "file_id", "aliquot_id")
        )
        file_df = self.sql_context.createDataFrame((file_row,), self.es_file_schema)
        document_df = self.sql_context.createDataFrame(
            (document_row,),
            types.StructType(
                [
                    types.StructField("did", types.StringType()),
                    *ascat.RawAscatStruct.fields,
                ]
            ),
        )

        config = mock.MagicMock()
        mock_sql_context = mock.MagicMock()
        doc_dataframe_util = _arrange_dataframe_util(document_df)
        es_dataframe_util = _arrange_dataframe_util(file_df)
        es_client = mock.MagicMock()
        iterate_es_results.return_value = ({"_source": {"file_id": "file-0"}},)

        builder = builders.AscatBuilder(
            config, mock_sql_context, doc_dataframe_util, es_dataframe_util, es_client
        )

        ascat_df = builder.build_from_scratch(primary_aliquot_df=primary_aliquot_df)

        assert ascat_df.count() == 1
        assert ascat_df.schema == self.final_ascat_schema
