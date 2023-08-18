import abc
import contextlib
import logging
from collections.abc import Iterable, Iterator, Mapping
from typing import Generic, TypeVar

import elasticsearch
import marshmallow
import toml
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration, constants, exporter
from mutation_indexer import logging as mutation_indexer_logging
from mutation_indexer.builders import base_builder, bases
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.configuration import indexd

logger = logging.getLogger(__name__)

TConfig = TypeVar("TConfig", bound=configuration.Configuration)


@contextlib.contextmanager
def initialize_spark() -> Iterator[sql.SparkSession]:
    """
    Initializes the spark session.

    Returns:
        The context manager for spark session for the current driver.
    """
    with sql.SparkSession.builder.getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session


def get_index_client(config: indexd.IndexD) -> client.IndexClient:
    """
    Builds the index client with the given configuration values.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An indexd client
    """
    return client.IndexClient(
        baseurl=f"{config.host}:{config.port}",
        auth=(config.user, config.password),
    )


def get_es_client(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    """
    builds the elastic search client based on the configuration.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An elasticsearch client
    """
    return elasticsearch.Elasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(
            config.user,
            config.password,
        ),
    )


class Driver(Generic[TConfig], abc.ABC):
    def __init__(self, config_schema: marshmallow.Schema) -> None:
        self._config_schema = config_schema

    @abc.abstractmethod
    def _get_builders(
        self,
        config: configuration.Configuration,
        spark_session: sql.SparkSession,
        es_client: elasticsearch.Elasticsearch,
        indexd: client.IndexClient,
    ) -> Iterable[bases.Builder]:
        pass

    def _get_index_builders(
        self, config: TConfig
    ) -> Mapping[constants.IndexType, base_builder.BaseBuilder]:
        return {}

    def run(self) -> None:
        mutation_indexer_logging.configure()

        try:
            config: TConfig = self._config_schema.load(  # type: ignore
                toml.load("configuration.toml")
            )

            mutation_indexer_logging.add_build_id(config.build.build_id)

            with get_es_client(
                config.elasticsearch.connection
            ) as es_client, initialize_spark() as spark_session:
                indexd = get_index_client(config.indexd)
                builders = self._get_builders(config, spark_session, es_client, indexd)
                old_builders = self._get_index_builders(config)
                index_exporter = exporter.Exporter(
                    spark_session.sparkContext,
                    config.build.index_types,
                    exporter.Builders(builders, old_builders),
                )

                index_exporter.run()
        except Exception as ex:
            logger.critical("Driver failed", exc_info=ex)
