import unittest
from pyspark import SparkContext
from pyspark.sql import SQLContext

class SparkTestCase(unittest.TestCase):

    def setUp(self):
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[2]', class_name)
        self.sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
        self.sqlContext = SQLContext(self.sc)
        log4j = self.sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    def tearDown(self):
        self.sc.stop()
        self.sc._jvm.System.clearProperty("spark.driver.port")

