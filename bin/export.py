import logging
import sys
import argparse
import ast
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
    parser.add_argument('-a', '--amendments',
                        help='File containing amendments to the configuration')
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

    if args.amendments:
        with open(args.amendments, 'r') as f:
            amendments = f.read()
        amendments = ast.literal_eval(amendments)

        config.amend(amendments)

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
