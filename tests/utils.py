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

        is_level_valid = validate_level(dict1[k], dict2[k], 'root.{}'.format(k))
        if is_level_valid:
            print 'Level-[{}]: [GOOD]'.format(k)
        else:
            print 'Level-[{}]: [BAD]'.format(k)


def validate_level(a, b, path='root'):
    print "\nValidating [{}]".format(path)
    print "^^^^^^^^^^^^^^^^"
    if type(a) != type(b):
        print "TYPES MISMATCH!"
        print type(a), type(b)
        return False
        import pdb
        pdb.set_trace()

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
                validate_level(a[key], b[key], '.'.join([path, key]))

    elif isinstance(a, list):  # [TODO] fix this case!
        if len(a) != len(b):
            print "LIST LENGTH MISMATCH!"
            print "List a ~ {} len={}\n List b ~ {} len={}".format(type(a),
                                                                   len(a),
                                                                   type(b),
                                                                   len(b))
            return False

        for i, val_a in enumerate(a):
            for j, val_b in enumerate(b):
                cur_path = path + ".List[a{},b{}/{},{}]".format(i, j, len(a), len(b))
                values_match = validate_level(val_a, val_b, cur_path)
                if values_match:
                    print '[NICE!] {}'.format(cur_path)
                    break
            if not values_match:
                print "BRANCH IS MISSING FROM LIST"
                K1 = set(val_a.keys())
                K2 = set(b[0].keys())
                print "Keys a - keys b:", K1 - K2
                print "Keys b - keys a:", K2 - K1
                return False
            else:
                print "[GOOD]"
                return True
    else:
        if a != b:
            print '{} > [VALUE MISMATCH]'.format(path)
            print '{} | NOT EQUALS | {}'.format(a, b)
            return False
        else:
            print "[GOOD]"
            return True
        return a == b


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
