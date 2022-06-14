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

root = logging.getLogger()
root.setLevel(logging.INFO)


def get_es_client(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    return elasticsearch.Elasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(
            config.user,
            config.password,
        ),
    )


def main():
    """
    Define the spark context and parse agruments into config
    """
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
        sql_context = sql.SQLContext(spark_session.sparkContext)
        config_adapter = old_config.ConfigAdapter(config, es_client, indexd)
        exporter = gdc_mutation_export.GDCMutationExport(
            spark_session.sparkContext, sql_context, config_adapter
        )

        exporter.run_export()


@contextlib.contextmanager
def initialize_spark() -> Iterator[sql.SparkSession]:
    """
    Makes a spark and sqlContext
    """
    with sql.SparkSession.builder.getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session


if __name__ == "__main__":
    # Execute Main functionality
    main()
