import argparse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from config import configs 
from config import BaseConfig
from exports.gdc_mutation_export import GDCMutationExport


def main():
    '''
    Define the spark context and parse agruments into config
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='The configuration set to run with',
                        type=str,
                        choices=configs.keys(),
                        default='BaseConfig')
    args = parser.parse_args()

    # Get config
    config = configs[args.config]()

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
