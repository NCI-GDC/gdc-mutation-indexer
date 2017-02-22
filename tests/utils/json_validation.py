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


class JSONValidator:
    """
    JSON validation helper
    """

    @classmethod
    def find_mismatches(cls, test_json, true_json, mode):
        """
        Finds mismatches between <:mode> fields of json docs
        :mode in ['list', 'dict']
        """
        assert mode in ['list', 'dict']
        test_stats = cls.get_stats(test_json, mode=mode)
        true_stats = cls.get_stats(true_json, mode=mode)

        mismatches = {}
        for path, value in true_stats.items():
            if path in test_stats:
                if value != test_stats[path]:
                    if mode == 'list':
                        mismatches[path] = [test_stats[path], value]
                    elif mode == 'dict':
                        mismatches[path] = [value - test_stats[path],
                                            test_stats[path] - value]
            else:
                mismatches[path] = [None, value]

        return mismatches

    @staticmethod
    def validate_path(tree, path, value):
        """
        Checks if :tree[:path] has :value characteristics
        (either element count or keys set)
        """
        steps = path.split('.')
        steps.reverse()

        while steps:
            step = steps.pop()
            try:
                if step == 'root':
                    result = tree
                elif step.find('[') != -1:
                    substeps = step.split('[')
                    result = result[substeps[0]][int(substeps[1][:-1])]
                else:
                    result = result[step]
            except KeyError:
                return False

        if isinstance(result, dict):
            return set(value) == set(result.keys())

        elif isinstance(result, list):
            return value == len(result)

    @staticmethod
    def get_stats(tree, mode='list'):
        """
        Calculates simple statistics for json file

        :mode in ['dict', 'list']

        if :mode == 'list':
            Returns a dict of items (path, length)
            for all JSON :tree <list> fields

        elif :mode == 'dict':
            Returns a dict of items (path, set(subtree_keys))
            for all JSON :tree <dict> fields
        """
        assert mode in ['dict', 'list']

        def get_stack_items(subtree, current_path):
            if isinstance(subtree, dict):
                return [[current_path + '.{}'.format(k), v]
                        for k, v in subtree.items()]
            elif isinstance(subtree, list):
                return [[current_path + '[{}]'.format(i), x]
                        for i, x in enumerate(subtree)]
            else:
                return []

        if mode == 'dict':
            tree_stats = {'root': set(tree.keys())}
        else:
            tree_stats = {}

        stack = tree.items()
        while stack:
            path, subtree = stack.pop()

            if mode == 'list':
                if isinstance(subtree, list):
                    tree_stats['root.' + path] = len(subtree)
            elif mode == 'dict':
                if isinstance(subtree, dict):
                    tree_stats['root.' + path] = set(subtree.keys())

            stack.extend(get_stack_items(subtree, path))

        return tree_stats
