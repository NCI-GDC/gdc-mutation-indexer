import asyncio
import contextlib
import functools
import itertools
import logging
from collections.abc import Iterable, Iterator
from typing import Any, AsyncContextManager, NamedTuple, Optional

import aiohttp
import more_itertools
import yarl
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import Self

from mutation_indexer import aioutils
from mutation_indexer.configuration import indexd

logger = logging.getLogger(__name__)

DOCUMENT_URL_SCHEMA = types.StructType(
    [
        types.StructField("did", types.StringType()),
        types.StructField("_input_file_name", types.StringType()),
    ]
)


class URLMetadata(NamedTuple):
    url: str
    type: str
    state: str

    @staticmethod
    def from_json(url: str, metadata: dict) -> "URLMetadata":
        return URLMetadata(
            url=url, type=metadata.get("type", ""), state=metadata.get("state", "")
        )


class Document(NamedTuple):
    did: str
    urls_metadata: Iterable[URLMetadata]

    @staticmethod
    def from_json(doc: dict) -> "Document":
        return Document(
            did=doc["did"],
            urls_metadata=tuple(
                itertools.starmap(
                    URLMetadata.from_json, doc.get("urls_metadata", {}).items()
                )
            ),
        )


class IndexClient(AsyncContextManager):
    __slots__ = ("_config", "_connector", "_context", "__session")

    def __init__(
        self, config: indexd.IndexD, connector: Optional[aiohttp.BaseConnector] = None
    ) -> None:
        self._config = config
        self._connector = connector
        self._context = contextlib.AsyncExitStack()
        self.__session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> Self:
        indexd_url = yarl.URL.build(
            scheme=self._config.scheme, host=self._config.host, port=self._config.port
        )

        self.__session = await self._context.enter_async_context(
            aiohttp.ClientSession(
                indexd_url,
                connector=self._connector,
                connector_owner=not self._connector,
                headers={"content-type": "application/json"},
            )
        )

        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self._context.aclose()

    @property
    def _session(self) -> aiohttp.ClientSession:
        assert self.__session, "Cannot access session before entering context."

        return self.__session

    async def get(self, dids: Iterable[str]) -> Iterator[Document]:
        dids = dids if isinstance(dids, (list, tuple)) else tuple(dids)

        try:
            async with self._session.post("/bulk/documents", json=dids) as response:
                if response.status == 404:
                    return iter(())

                response.raise_for_status()

                data = await response.json(content_type=None)

            return map(Document.from_json, data)
        except aiohttp.ClientOSError:
            logger.warning(f"Failed to get docs: %s", dids)
            raise


class DocumentUrl(NamedTuple):
    did: str
    url: str


def _get_url(doc: Document) -> Optional[DocumentUrl]:
    """Select main IndexD url if one exist and format it to something that Spark
    understands

    Args:
        doc: IndexD document to extract URL from

    Returns:
        str: formatted main URL
    """
    for metadata in doc.urls_metadata:
        if metadata.type == "cleversafe" and metadata.state == "validated":
            url = metadata.url.replace("s3://", "s3a://").replace(
                "cleversafe.service.consul/", ""
            )

            return DocumentUrl(doc.did, url)

    logger.warning("File is missing: '{}'".format(doc.did))

    return None


class DataFrameUtil:
    __slots__ = ("_indexd", "_spark_session")

    def __init__(
        self,
        indexd: IndexClient,
        spark_session: sql.SparkSession,
    ):
        self._indexd = indexd
        self._spark_session = spark_session

    async def _get_urls(self, dids: Iterable[str]) -> Iterable[DocumentUrl]:
        docs = await self._indexd.get(dids)

        return tuple(filter(None, map(_get_url, docs)))

    async def _get_dataframe(
        self,
        schema: Optional[types.StructType],
        include_did: bool,
        enforce_schema: bool,
        has_header: bool,
        comment: Optional[str],
        dids: Iterable[str],
    ) -> sql.DataFrame:
        urls = await self._get_urls(dids)

        df = self._spark_session.read.csv(
            [u.url for u in urls],
            schema=schema,
            sep="\t",
            comment=comment,
            header=has_header,
            enforceSchema=enforce_schema,
            mode="FAILFAST",
        )

        if include_did:
            did_df = self._spark_session.createDataFrame(
                urls, schema=DOCUMENT_URL_SCHEMA
            )
            df = (
                df.withColumn("_input_file_name", F.input_file_name())
                .join(did_df, on="_input_file_name")
                .drop("_input_file_name")
            )

        return df

    async def get_dataframe(
        self,
        doc_ids: Iterable[str],
        schema: Optional[types.StructType] = None,
        batch_size: int = 200,
        include_document_ids: bool = True,
        enforce_schema: bool = True,
        has_header: bool = True,
        comment: Optional[str] = None,
    ) -> sql.DataFrame:
        def union(df0: sql.DataFrame, df1: sql.DataFrame) -> sql.DataFrame:
            return df0.union(df1)

        batches = more_itertools.ichunked(doc_ids, batch_size)
        get_dataframe = functools.partial(
            self._get_dataframe,
            schema,
            include_document_ids,
            enforce_schema,
            has_header,
            comment,
        )
        dfs = await asyncio.gather(*map(get_dataframe, batches))

        if dfs:
            return functools.reduce(union, dfs)
        elif schema:
            return self._spark_session.createDataFrame((), schema)

        raise ValueError("No documents found to load.")
