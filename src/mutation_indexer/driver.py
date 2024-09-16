import abc
import contextlib
import logging
from collections.abc import Iterator
from typing import ContextManager

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration
from mutation_indexer import logging as mutation_indexer_logging
from mutation_indexer.builders import bases
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.configuration import indexd
from mutation_indexer.constants import build

logger = logging.getLogger("mutation_indexer")


@contextlib.contextmanager
def load_spark_session() -> Iterator[sql.SparkSession]:
    """
    Initializes the spark session.

    Returns:
        The context manager for spark session for the current driver.
    """
    with sql.SparkSession.builder.getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session


def load_index_client(config: indexd.IndexD) -> client.IndexClient:
    """
    Builds the index client with the given configuration values.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An indexd client
    """
    return client.IndexClient(
        baseurl=f"{config.host}:{config.port}",  # type: ignore
        auth=(config.user, config.password),  # type: ignore
    )  # type: ignore


def load_es_client(
    config: es_config.Connection,
) -> ContextManager[elasticsearch.Elasticsearch]:
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
        http_auth=(config.user, config.password),
    )


class Driver(abc.ABC):
    @abc.abstractmethod
    def _load_builders(
        self, config: configuration.Configuration
    ) -> Iterator[bases.Builder]:
        raise NotImplementedError()

    def run(self) -> None:
        try:
            config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(  # type: ignore
                toml.load("configuration.toml")
            )

            mutation_indexer_logging.add_build_id(config.build.build_id)

            builders = self._load_builders(config)
            inputs: dict[str, sql.DataFrame] = {}

            for builder in builders:
                inputs[build.DataFrame.to_param(builder.output)] = builder.build(
                    **inputs
                )
        except Exception as ex:
            logger.critical("Driver failed", exc_info=ex)
