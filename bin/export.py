import logging

from config import BaseConfig as Config
from exports import gdc_mutation_export
from pyspark import sql

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    """
    Define the spark context and parse agruments into config
    """
    config = Config()

    with get_spark_session(config) as spark_session:
        exporter = gdc_mutation_export.GDCMutationExport(spark_session, config)

        exporter.run_export()


def get_spark_session(config) -> sql.SparkSession:
    """
    Creates the spark session to be used by the application.
    """
    spark_session = sql.SparkSession.builder.master(config.master).appName(config.name).config("log4j.rootCategory", "FATAL").getOrCreate()

    return spark_session


if __name__ == '__main__':
    # Execute Main functionality
    main()
