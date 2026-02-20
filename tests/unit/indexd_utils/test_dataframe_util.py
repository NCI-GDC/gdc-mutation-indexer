import itertools
from collections.abc import Iterable
from typing import NamedTuple, TypedDict
from unittest import mock

import pytest
from indexclient import client
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import indexd_utils


class UrlMetadata(TypedDict):
    type: str
    state: str


def arrange_url_metadata(type: str = "cleversafe", state: str = "validated") -> UrlMetadata:
    return UrlMetadata(type=type, state=state)


def arrange_document(
    did="file-0", urls_metadata: dict[str, UrlMetadata] | None = None
) -> client.Document:
    urls_metadata = (
        {"file://file-0.format": arrange_url_metadata()}
        if urls_metadata is None
        else urls_metadata
    )
    json = {"did": did, "urls_metadata": urls_metadata}

    return client.Document(None, did, json)


class DocumentContent(NamedTuple):
    did: str = "file-0"
    data: tuple[str, ...] = ("a", "b")


def stub_input_file_name() -> sql.Column:
    return F.concat(
        F.lit("file://"),
        F.element_at(F.split("doc_data", "\\."), 1),
        F.lit(".format"),
    )


class TestDataFrameUtil:
    @pytest.fixture(autouse=True)
    def import_fixtures(self, data_dir: str, spark_session: sql.SparkSession) -> None:
        self.data_dir = data_dir
        self.spark_session = spark_session

    def arrange_index_client(
        self,
        documents: Iterable[Iterable[client.Document] | None] = ((arrange_document(),),),
    ) -> mock.MagicMock:
        index_client = mock.MagicMock()

        index_client.bulk_request.side_effect = documents

        return index_client

    def arrange_data_rows(
        self, document_content: DocumentContent
    ) -> Iterable[tuple[str, ...]]:
        for data in document_content.data:
            yield (f"{document_content.did}.{data}",)

    def arrange_sql_context(
        self,
        document_contents: Iterable[Iterable[DocumentContent]] = ((DocumentContent(),),),
        schema: tuple[str, ...] | types.StructType | None = ("doc_data",),
    ) -> mock.MagicMock:
        sql_context = mock.MagicMock()
        data = (
            itertools.chain.from_iterable(self.arrange_data_rows(d) for d in ds)
            for ds in document_contents
        )

        sql_context.createDataFrame.side_effect = self.spark_session.createDataFrame
        sql_context.read.csv.side_effect = (
            self.spark_session.createDataFrame(tuple(d), schema) for d in data
        )

        return sql_context

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__default_settings(self, input_file_name: mock.MagicMock) -> None:
        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client()
        sql_context = self.arrange_sql_context()
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0",))
        result_rows = result_df.collect()

        indexd.bulk_request.assert_called_once_with(["file-0"])
        sql_context.read.csv.assert_called_once_with(
            ["file://file-0.format"],
            schema=None,
            sep="\t",
            comment=None,
            header=True,
            enforceSchema=True,
            mode="FAILFAST",
        )

        assert result_df.columns == ["doc_data", "did"]
        assert len(result_rows) == 2
        assert all(row.did == "file-0" for row in result_rows)

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__multiple_files(self, input_file_name: mock.MagicMock) -> None:
        documents = (
            arrange_document(),
            arrange_document(
                "file-1",
                urls_metadata={"file://file-1.format": arrange_url_metadata()},
            ),
        )
        document_data = (DocumentContent(), DocumentContent("file-1", ("c", "d")))

        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client((documents,))
        sql_context = self.arrange_sql_context((document_data,))
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0", "file-1"))
        result_rows = {
            key: tuple(items)
            for key, items in itertools.groupby(result_df.collect(), lambda row: row.did)
        }

        indexd.bulk_request.assert_called_once_with(["file-0", "file-1"])

        assert result_df.columns == ["doc_data", "did"]
        assert result_rows.keys() == frozenset(("file-0", "file-1"))
        assert len(result_rows["file-0"]) == 2
        assert frozenset(row.doc_data for row in result_rows["file-0"]) == frozenset(
            ("file-0.a", "file-0.b")
        )
        assert len(result_rows["file-1"]) == 2
        assert frozenset(row.doc_data for row in result_rows["file-1"]) == frozenset(
            ("file-1.c", "file-1.d")
        )

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__batching(self, input_file_name: mock.MagicMock) -> None:
        documents = (
            (arrange_document(),),
            (
                arrange_document(
                    "file-1",
                    urls_metadata={"file://file-1.format": arrange_url_metadata()},
                ),
            ),
        )
        document_data = ((DocumentContent()),), (DocumentContent("file-1", ("c", "d")),)

        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client(documents)
        sql_context = self.arrange_sql_context(document_data)
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        util.get_dataframe(("file-0", "file-1"), index_batch_size=1, csv_batch_size=1)

        indexd.bulk_request.assert_has_calls(
            (mock.call(["file-0"]), mock.call(["file-1"])), any_order=True
        )
        sql_context.read.csv.assert_has_calls(
            (
                mock.call(
                    ["file://file-0.format"],
                    comment=None,
                    enforceSchema=True,
                    header=True,
                    mode="FAILFAST",
                    schema=mock.ANY,
                    sep="\t",
                ),
                mock.call(
                    ["file://file-1.format"],
                    comment=None,
                    enforceSchema=True,
                    header=True,
                    mode="FAILFAST",
                    schema=mock.ANY,
                    sep="\t",
                ),
            ),
            any_order=True,
        )

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__bulk_request_returns_none(
        self, input_file_name: mock.MagicMock
    ) -> None:
        input_file_name.side_effect = stub_input_file_name
        schema = types.StructType([types.StructField("doc_data", types.StringType())])
        indexd = self.arrange_index_client(None)
        sql_context = self.arrange_sql_context(((),), schema)
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0", "file-1"))

        indexd.bulk_request.assert_called_once_with(["file-0", "file-1"])

        assert result_df.count() == 0

    @pytest.mark.parametrize(
        ("type", "state"),
        (("archive", "validated"), ("cleversafe", "blocked")),
        ids=("invalid_type", "invalid_state"),
    )
    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__filter_url(
        self, input_file_name: mock.MagicMock, type: str, state: str
    ) -> None:
        documents = (
            arrange_document(
                urls_metadata={"file://file-0.format": arrange_url_metadata(type, state)},
            ),
        )

        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client((documents,))
        schema = types.StructType([types.StructField("doc_data", types.StringType())])
        sql_context = self.arrange_sql_context(((),), schema)
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0",), schema)

        indexd.bulk_request.assert_called_once_with(["file-0"])
        sql_context.read.csv.assert_called_once_with(
            [],
            schema=schema,
            sep="\t",
            comment=None,
            header=True,
            enforceSchema=True,
            mode="FAILFAST",
        )

        assert result_df.count() == 0

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__url_formated(self, input_file_name: mock.MagicMock) -> None:
        documents = (
            arrange_document(
                "file-0",
                urls_metadata={
                    "s3://cleversafe.service.consul/file-0.format": arrange_url_metadata()
                },
            ),
        )

        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client((documents,))
        sql_context = self.arrange_sql_context()
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0",), include_document_ids=False)

        indexd.bulk_request.assert_called_once_with(["file-0"])
        sql_context.read.csv.assert_called_once_with(
            ["s3a://file-0.format"],
            schema=None,
            sep="\t",
            comment=None,
            header=True,
            enforceSchema=True,
            mode="FAILFAST",
        )

        assert result_df.count() == 2

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__exclude_dids(self, input_file_name: mock.MagicMock) -> None:
        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client()
        sql_context = self.arrange_sql_context()
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        result_df = util.get_dataframe(("file-0",), include_document_ids=False)

        indexd.bulk_request.assert_called_once_with(["file-0"])
        sql_context.read.csv.assert_called_once_with(
            ["file://file-0.format"],
            schema=None,
            sep="\t",
            comment=None,
            header=True,
            enforceSchema=True,
            mode="FAILFAST",
        )

        assert result_df.count() == 2
        assert "did" not in result_df.columns

    @mock.patch("pyspark.sql.functions.input_file_name")
    def test__get_dataframe__options_passed_to_reader(
        self, input_file_name: mock.MagicMock
    ) -> None:
        input_file_name.side_effect = stub_input_file_name
        indexd = self.arrange_index_client()
        sql_context = self.arrange_sql_context()
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

        schema = mock.MagicMock()
        comment = mock.MagicMock()
        has_header = mock.MagicMock()
        enforce_schema = mock.MagicMock()

        result_df = util.get_dataframe(
            ("file-0",),
            schema=schema,
            comment=comment,
            has_header=has_header,
            enforce_schema=enforce_schema,
        )

        indexd.bulk_request.assert_called_once_with(["file-0"])
        sql_context.read.csv.assert_called_once_with(
            ["file://file-0.format"],
            schema=schema,
            sep="\t",
            comment=comment,
            header=has_header,
            enforceSchema=enforce_schema,
            mode="FAILFAST",
        )

        assert result_df.count() == 2
