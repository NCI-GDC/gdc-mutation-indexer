import contextlib
import glob
import logging
from os import path
from typing import Iterator

import pyspark
import toml
from pyspark import sql

from exports import configuration, gdc_mutation_export
from exports.configuration import spark
import config as old_config

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    """
    Define the spark context and parse agruments into config
    """
    files_dir = pyspark.SparkFiles().getRootDirectory()
    config_file = glob.glob(path.join(files_dir, "mutation-indexer-config-*.toml"))[0]
    config: configuration.Configuration = configuration.CONGIF_SCHEMA.load(
        toml.load(config_file)
    )

    with initialize_spark(config.spark_arguments) as spark_session:
        sql_context = sql.SQLContext(spark_session.sparkContext)
        config_adapter = old_config.ConfigAdapter(config)
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
