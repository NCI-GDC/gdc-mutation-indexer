import asyncio
import contextlib
import functools
import itertools
import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from typing import Any, AsyncContextManager, ContextManager

import elasticsearch
import gdcmodels
import importlib_resources as resources
import toml
import yaml
from elasticsearch import helpers
from typing_extensions import Self
from pyspark import sql

from mutation_indexer import configuration, main
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.constants import build


def get_es_client(config: es_config.Connection) -> elasticsearch.AsyncElasticsearch:
    return elasticsearch.AsyncElasticsearch(
        config.nodes.split("."),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(config.user, config.password),
    )


@contextlib.contextmanager
def _load_configuration() -> Iterator[configuration.Configuration]:
    with resources.as_file(
        resources.files("tests.async_integration").joinpath("data/inputs/files")
    ) as input_dir, tempfile.TemporaryDirectory() as tmp_dir:
        cytobands_file = str(input_dir / "genes.cytobands.tsv.gz")
        census_file = str(input_dir / "cancer_gene_census_set.tsv.gz")
        gene_model_file = str(input_dir / "genes.ndjson.gz")

        data = toml.loads(resources.read_text("mutation_indexer", "configuration.toml"))
        data["elasticsearch"]["connection"]["nodes"] = os.environ.get(
            "ES_NODES", data["elasticsearch"]["connection"]["nodes"]
        )
        data["builders"]["viz"]["gene_model"]["cytobands_file"] = cytobands_file
        data["builders"]["viz"]["gene_model"]["census_file"] = census_file
        data["builders"]["viz"]["gene_model"]["gene_model_file"] = gene_model_file

        yield configuration.CONFIG_SCHEMA.load(data)  # type: ignore


class ConfigurationSetup(ContextManager[configuration.Configuration]):
    def __init__(self) -> None:
        self._context = contextlib.ExitStack()

    def __enter__(self) -> configuration.Configuration:
        try:
            return self._context.enter_context(_load_configuration())
        except:
            self._context.close()
            raise

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._context.close()


class ESIndicesSetup(AsyncContextManager):
    ID_FIELDS = {build.IndexType.FILE: "file_id", build.IndexType.CASE: "case_id"}

    def __init__(self, config: es_config.Elasticsearch) -> None:
        self._config = config
        self._context = contextlib.AsyncExitStack()
        self._models = gdcmodels.get_es_models()["gdc_from_graph"]

    def _load_data(self, index_type: build.IndexType) -> Iterator:
        index_resource = (
            resources.files("tests.async_integration")
            / f"data/inputs/elasticsearch/{index_type.name.lower()}"
        )

        with resources.as_file(index_resource) as folder:
            for file in folder.glob("**/*.yaml"):
                yield from yaml.load(file.read_bytes(), Loader=yaml.CLoader) or ()

    async def _load_index(
        self,
        es_client: elasticsearch.AsyncElasticsearch,
        index_type: build.IndexType,
        index: str,
    ) -> None:
        model = self._models[index_type.name.lower()]
        data = self._load_data(index_type)
        id_field = self.ID_FIELDS[index_type]
        actions = ({"_id": d[id_field], "_index": index, "_source": d} for d in data)

        await es_client.indices.create(
            index=index, mappings=model.mappings, settings=model.settings, ignore=[400]
        )
        await helpers.async_bulk(es_client, actions)

    @contextlib.asynccontextmanager
    async def _load_indices(
        self, es_client: elasticsearch.AsyncElasticsearch
    ) -> AsyncIterator[None]:
        indices = {
            build.IndexType.FILE: self._config.read.file_index,
            build.IndexType.CASE: self._config.read.case_index,
        }

        await asyncio.gather(
            *itertools.starmap(
                functools.partial(self._load_index, es_client), indices.items()
            )
        )
        await es_client.indices.refresh(index=[i for i in indices.values()])

        yield

        await es_client.indices.delete(index=[i for i in indices.values()])

    async def __aenter__(self) -> Self:
        try:
            es_client = await self._context.enter_async_context(
                get_es_client(self._config.connection)
            )
            _ = await self._context.enter_async_context(self._load_indices(es_client))
        except:
            await self._context.aclose()
            raise

        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self._context.aclose()


class SparkSessionSetup(ContextManager[sql.SparkSession]):
    def __init__(self) -> None:
        self._context = contextlib.ExitStack()

    def __enter__(self) -> sql.SparkSession:
        spark_session = self._context.enter_context(
            sql.SparkSession.builder.master("local[*]")
            .appName("sqlContextFixture")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.ui.showConsoleProgress", False)
            .config("spark.ui.enabled", False)
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )

        spark_session.sparkContext.setLogLevel("FATAL")
        spark_session.sql("set spark.sql.caseSensitive=true")

        return spark_session

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._context.close()


class MutationIndexerSetup(AsyncContextManager):
    def __init__(self, config: configuration.Configuration) -> None:
        self._config = config

    async def __aenter__(self) -> Self:
        await main._main(self._config)

        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        pass
