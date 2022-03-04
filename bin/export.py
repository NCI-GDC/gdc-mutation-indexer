import logging

import pyspark
from pyspark import sql

from mutation_indexer import config, gdc_mutation_export

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    """
    Define the spark context and parse agruments into config
    """
    conf = config.Config()

    sc, sqlContext = make_spark_context(conf)

    exporter = gdc_mutation_export.GDCMutationExport(sc, sqlContext, conf)

    exporter.run_export()

    # Tear down actions
    sc.stop()


def make_spark_context(config):
    """
    Makes a spark and sqlContext
    """
    conf = pyspark.SparkConf().setAppName(config.name)
    conf = conf.setMaster(config.master)
    sc = pyspark.SparkContext(conf=conf, pyFiles=[])
    sqlContext = sql.SQLContext(sc)
    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    return sc, sqlContext


if __name__ == "__main__":
    # Execute Main functionality
    main()
