import abc
import argparse
import contextlib
import logging
import runpy
from collections.abc import Iterable, Iterator
from typing import ContextManager, Optional, cast

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration
from mutation_indexer import logging as mutation_indexer_logging
from mutation_indexer.builders import bases
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.configuration import indexd
from mutation_indexer.constants import app, build

logger = logging.getLogger(__name__)


class Driver(abc.ABC):
    """A base for various submodule drivers to build a collection of data in Spark."""

    @classmethod
    @contextlib.contextmanager
    def load_spark_session(cls) -> Iterator[sql.SparkSession]:
        """
        Loads the spark session.

        Returns:
            The context manager for spark session for the current driver.
        """
        with sql.SparkSession.builder.getOrCreate() as spark_session:
            spark_session.sparkContext.setLogLevel("FATAL")

            yield spark_session

    @classmethod
    def load_index_client(cls, config: indexd.IndexD) -> client.IndexClient:
        """
        Loads the index client with the given configuration values.

        Args:
            config: The connection configuration for setting up the client.

        Returns:
            An indexd client
        """
        return client.IndexClient(
            baseurl=f"{config.host}:{config.port}",  # type: ignore
            auth=(config.user, config.password),  # type: ignore
        )  # type: ignore

    @classmethod
    def load_es_client(
        cls,
        config: es_config.Connection,
    ) -> ContextManager[elasticsearch.Elasticsearch]:
        """
        Loads the elastic search client based on the configuration.

        Args:
            config: The connection configuration for setting up the client.

        Returns:
            A context wrapping an elasticsearch client
        """

        return elasticsearch.Elasticsearch(
            config.nodes.split(","),
            use_ssl=config.use_ssl,
            verify_certs=config.verify_certs,
            http_auth=(config.user, config.password),
        )

    @classmethod
    @abc.abstractmethod
    def _load_builders(
        cls, config: configuration.Configuration
    ) -> ContextManager[Iterable[bases.Builder]]:
        """Loads the builders that need to be run by the driver.

        NOTE: These drivers MUST be loaded in topological orders.

        Args:
            config: The configuration associated with the this run of the driver.

        Returns:
            A context manager containing an iterable of the builders in topological
            order.
        """
        raise NotImplementedError()

    @classmethod
    def run(cls, config: Optional[configuration.Configuration] = None) -> None:
        """A function for running the spark driver to build the desired data."""
        try:
            config = config or cast(
                configuration.Configuration,
                configuration.CONFIG_SCHEMA.load(toml.load("configuration.toml")),
            )

            mutation_indexer_logging.add_build_id(config.build.build_id)

            with cls._load_builders(config) as builders:
                inputs: dict[str, sql.DataFrame] = {}

                logger.info("Build %s started.", config.build.build_id)

                for builder in builders:
                    inputs[build.DataFrame.to_param(builder.output)] = builder.build(
                        **inputs
                    )

                logger.info("Build %s completed.", config.build.build_id)
        except Exception as ex:
            logger.critical("Driver failed", exc_info=ex)


def get_argument_parser() -> argparse.ArgumentParser:
    """
    Get the argument parser for the driver application.

    Returns:
        An argument parser with the required parameters to run the driver application.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("driver", type=str, choices=app.DRIVERS, required=True)

    return parser


if __name__ == "__main__":
    try:
        parser = get_argument_parser()
        args = parser.parse_args()

        runpy.run_module(app.DRIVERS[args.driver], run_name="__main__", alter_sys=True)
    except Exception as ex:
        logger.critical("Call to driver module failed.", exc_info=ex)
