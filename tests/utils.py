import unittest
from pyspark import SparkContext
from pyspark.sql import SQLContext

class SparkTestCase(unittest.TestCase):

    def setUp(self):
        class_name = self.__class__.__name__

        self.sc = SparkContext('local[*]', class_name)
        self.sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
        self.sqlContext = SQLContext(self.sc)
        log4j = self.sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    def tearDown(self):
        self.sc.stop()
        self.sc._jvm.System.clearProperty("spark.driver.port")


def match_dictionaries(dict1, dict2):
    for k in dict1:
        if isinstance(dict1[k], dict):
            print '\n[{}] => Dict value'.format(k)
        elif isinstance(dict1[k], list):
            print '\n[{}] => List value'.format(k)
        else:
            print '\n[{}] => Primitive value'.format(k)

        is_level_valid = validate_level(dict1[k], dict2[k])
        if is_level_valid:
            print 'Level-[{}]: [GOOD]'.format(k)
        else:
            print 'Level-[{}]: [BAD]'.format(k)


def validate_level(a, b):
    if isinstance(a, dict):
        K1 = set(a.keys())
        K2 = set(b.keys())
        if K1 != K2:
            print 'Keys mismatch!'
            print K1 - K2
            print K2 - K1
            return False
        else:
            for key in a:
                validate_level(a[key], b[key])

    elif isinstance(a, list):  # [TODO] fix this case!
        for value in a:
            if value not in b:
                print "{} BRANCH IS MISSING".format(type(value))
                import pdb
                pdb.set_trace()
                return False
            else:
                return True
    else:
        if a != b:
            print 'Value mismatch!'
            print '{} | NOT EQUALS | {}'.format(a, b)
        return a == b

