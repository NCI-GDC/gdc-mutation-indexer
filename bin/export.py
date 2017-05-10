import logging
import sys
import argparse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from exports.gdc_mutation_export import GDCMutationExport

root = logging.getLogger()
root.setLevel(logging.INFO)


def main():
    '''
    Define the spark context and parse agruments into config
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='The configuration set to run with',
                        type=str,
                        choices=['Base', 'Test'],
                        default='Base')
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # Get config
    if args.config == 'Test':
        from tests_config import TestConfig as Config
    else:
        from config import BaseConfig as Config

    config = Config()
    config.config_type = args.config

    sc, sqlContext = make_spark_context(config)

    exporter = GDCMutationExport(sc, sqlContext, config)

    exporter.run_export(config)
        
    # Tear down actions
    sc.stop()

def make_spark_context(config):
    '''
    Makes a spark and sqlContext
    '''
    conf = SparkConf().setAppName(config.app_name)
    if config.config_type == 'Test':
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(config.spark_master)
    sc = SparkContext(conf=conf, pyFiles=[])
    sqlContext = SQLContext(sc)
    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    return sc, sqlContext

if __name__ == '__main__':
    # Execute Main functionality
    main()
