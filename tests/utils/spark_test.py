import json
import unittest
from pyspark import SparkContext
from pyspark.sql import SQLContext


class SparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        class_name = cls.__name__

        cls.sc = SparkContext('local[*]', class_name)
        cls.sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
        cls.sqlContext = SQLContext(cls.sc)
        log4j = cls.sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        cls.sc._jvm.System.clearProperty("spark.driver.port")
