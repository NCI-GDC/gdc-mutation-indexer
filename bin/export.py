import contextlib
import logging
from typing import Iterator

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

import config as old_config
from exports import configuration, gdc_mutation_export
from exports.configuration import elasticsearch as es_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_es_client(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    """
    builds the elastic search client based on the configuration.

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


def main():
    config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(
        toml.load("configuration.toml")
    )
    indexd = client.IndexClient(
        baseurl=f"{config.indexd.host}:{config.indexd.port}",
        auth=(config.indexd.user, config.indexd.password),
    )

    with get_es_client(
        config.elasticsearch.connection
    ) as es_client, initialize_spark() as spark_session:
        sql_context = sql.SQLContext(
            spark_session.sparkContext, sparkSession=spark_session
        )
        config_adapter = old_config.ConfigAdapter(config, es_client, indexd)
        exporter = gdc_mutation_export.GDCMutationExport(
            spark_session.sparkContext, sql_context, config_adapter
        )

        exporter.run_export()


if __name__ == "__main__":
    try:
        main()
    except:
        logger.critical("Driver failed.", exc_info=True)
