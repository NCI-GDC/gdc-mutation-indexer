import contextlib
from typing import Iterator

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

from mutation_indexer.core import configuration
from mutation_indexer.core.configuration import elasticsearch as es_config
from mutation_indexer.core.configuration import indexd, spark


def get_configuration() -> configuration.Configuration:
    config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(
        toml.load("config.toml")
    )

    return config


def get_elasticsearch(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    return elasticsearch.Elasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(
            config.user,
            config.password,
        ),
    )


def get_indexd(config: indexd.IndexD) -> client.IndexClient:
    return client.IndexClient(
        baseurl=f"{config.indexd.host}:{config.indexd.port}",
        auth=(config.indexd.user, config.indexd.password),
    )


def get_conifg_adapter(
    config: configuration.Configuration,
) -> configuration.ConfigAdapter:
    es_client = get_elasticsearch(config.elasticsearch.connection)
    indexd = get_indexd(config.indexd)

    return configuration.ConfigAdapter(config, es_client, indexd)


@contextlib.contextmanager
def initialize_spark(spark_arguments: spark.Arguments) -> Iterator[sql.SparkSession]:
    """
    Makes a spark and sqlContext
    """
    with sql.SparkSession.builder.master(spark_arguments.master).appName(
        spark_arguments.name
    ).getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session
