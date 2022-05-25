import contextlib
import logging
from typing import Iterator

import toml
import elasticsearch
from indexclient import client
from pyspark import sql

import config as old_config
from exports import configuration, gdc_mutation_export
from exports.configuration import spark

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    """
    Define the spark context and parse agruments into config
    """
    config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(
        toml.load("config.toml")
    )
    es_client = elasticsearch.Elasticsearch(
        config.elasticsearch.connection.nodes.split(","),
        use_ssl=config.elasticsearch.connection.use_ssl,
        verify_certs=config.elasticsearch.connection.verify_certs,
        http_auth=(
            config.elasticsearch.connection.user,
            config.elasticsearch.connection.password,
        ),
    )
    indexd = client.IndexClient(
        baseurl=f"{config.indexd.host}:{config.indexd.port}",
        auth=(config.indexd.user, config.indexd.password),
    )

    with initialize_spark(config.spark_arguments) as spark_session:
        sql_context = sql.SQLContext(spark_session.sparkContext)
        config_adapter = old_config.ConfigAdapter(config, es_client, indexd)
        exporter = gdc_mutation_export.GDCMutationExport(
            spark_session.sparkContext, sql_context, config_adapter
        )

        exporter.run_export()


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


if __name__ == "__main__":
    # Execute Main functionality
    main()
