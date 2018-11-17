import logging
import argparse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from exports.gdc_mutation_export import GDCMutationExport
from config import BaseConfig as Config

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    """
    Define the spark context and parse agruments into config
    """
    config = Config()

    sc, sqlContext = make_spark_context(config)

    exporter = GDCMutationExport(sc, sqlContext, config)

    exporter.run_export()

    # Tear down actions
    sc.stop()


def make_spark_context(config):
    """
    Makes a spark and sqlContext
    """
    conf = SparkConf().setAppName(config.name)
    sc = SparkContext(conf=conf, pyFiles=[])
    sqlContext = SQLContext(sc)
    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    return sc, sqlContext


if __name__ == '__main__':
    # Execute Main functionality
    main()
