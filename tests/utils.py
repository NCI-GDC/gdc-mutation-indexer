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


class DiffObject:
    def __init__(self, address, diff_values, message):
        self.address = address
        self.diff_values = diff_values
        self.message = message

    def __str__(self):
        output_dict = {'address': self.address,
                       'diff': self.diff_values,
                       'message': self.message
                       }
        return str(output_dict)

    def __repr__(self):
        return self.__str__()


LEVEL_SEPARATOR = "#"
KEY_VALUE_SEPARATOR = "="


class TestJsonObject:
    @classmethod
    def assert_equal(cls, address, value, other, message):
        if value != other:
            return [DiffObject(address, [value, other], message)]
        return []

    @classmethod
    def assert_in(cls, address, value, other, message):
        if value not in other:
            return [DiffObject(address, [value, other], message)]
        return []

    @classmethod
    def validate_transcript_list(cls, address, list_transcripts, other_list_transcripts):
        return cls.validate_two_list_jsons(address, list_transcripts, other_list_transcripts,
                                           ["transcript_id"],
                                           cls.validate_two_transcripts, "transcript")

    @classmethod
    def validate_case_list(cls, address, list_cases, other_list_cases):
        return cls.validate_two_list_jsons(address, list_cases, other_list_cases,
                                           ["submitter_id"],
                                           cls.validate_two_cases, "case")

    @classmethod
    def build_dict_from_list_json(cls, list_json, identity_fields, object_name=None):
        res_dict = {}
        for js in list_json:
            key = cls.identity_from_identity_fields(js, identity_fields, object_name)
            js_content = js
            if object_name:
                js_content = js[object_name]
            res_dict[key] = js_content
        return res_dict

    @classmethod
    def identity_from_identity_fields(cls, json, identity_fields, object_name=None):
        if object_name is not None:
            json = json[object_name]
        identity = ''
        for key in identity_fields:
            identity += str(json[key]) + '-'
        identity = identity[:-1]
        return identity

    @classmethod
    def validate_two_flat_lists(cls, address, f_list, other_list):
        res = []
        f_len = len(f_list)
        other_len = len(other_list)
        res.extend(cls.assert_equal(address, f_len, other_len,
                                    "first list has {0} items while second list has {1} items"
                                    .format(f_len, other_len)))
        s = set(f_list)
        for item in other_list:
            res.extend(cls.assert_in(address, item, other_list, "{0} does not exists in {1}".format(item, s)))
        return res

    @classmethod
    def validate_two_flat_jsons(cls, address, json_obj, other_json_obj):
        res = []
        for field in json_obj.keys():
            new_address = address + "{}{}".format(LEVEL_SEPARATOR, field)
            diff = cls.assert_in(new_address, field, other_json_obj.keys(),
                                 "{0} does not exists in {1}".format(field, other_json_obj.keys()))
            if diff:
                res.extend(diff)
                continue
            else:
                res.extend(cls.assert_equal(new_address, json_obj[field], other_json_obj[field],
                                            "field {0}: {1} is not equal {2}"
                                            .format(field, json_obj[field], other_json_obj[field])))
        return res

    @classmethod
    def validate_two_list_jsons(cls, address, list_jsons, other_list_jsons, identity_fields,
                                diff_func=None, object_name=None):
        res = []
        address += "{}{}".format(LEVEL_SEPARATOR, object_name)
        dict_jsons = cls.build_dict_from_list_json(list_jsons, identity_fields, object_name)
        other_dict_jsons = cls.build_dict_from_list_json(other_list_jsons, identity_fields, object_name)
        first_size = len(dict_jsons.keys())
        second_size = len(other_dict_jsons.keys())

        res.extend(cls.assert_equal(address, first_size, second_size,
                                    "First list has {0} items while second list has {1} items"
                                    .format(first_size, second_size)))
        for key in dict_jsons.keys():
            new_address = address + "{}{}".format(KEY_VALUE_SEPARATOR, key)
            diff = cls.assert_in(new_address, key, other_dict_jsons.keys(),
                                 "{0} is not in {1}".format(key, other_dict_jsons.keys()))
            if diff:
                res.extend(diff)
                continue
            if diff_func:
                res.extend(diff_func(new_address, dict_jsons[key], other_dict_jsons[key]))
        return res

    @classmethod
    def validate_two_nested_jsons(cls, address, json_obj, other_json_obj, ignored_list=None):
        res = []
        if ignored_list is None:
            ignored_list = []
        for field in json_obj.keys():
            new_address = address + "{}{}".format(LEVEL_SEPARATOR, field)
            diff = cls.assert_in(new_address, field, other_json_obj,
                                 "{0} is not in {1}".format(field, other_json_obj.keys()))
            if diff:
                res.extend(diff)
                continue
            elif field not in ignored_list:
                if type(json_obj[field]) is list:
                    res.extend(cls.validate_two_flat_lists(new_address, json_obj[field], other_json_obj[field]))
                elif type(json_obj[field]) is dict:
                    res.extend(cls.validate_two_nested_jsons(new_address, json_obj[field], other_json_obj[field]))
                else:
                    res.extend(cls.assert_equal(new_address, json_obj[field], other_json_obj[field],
                                                "field {0}: {1} is not equal {2}"
                                                .format(field, json_obj[field], other_json_obj[field])))
        return res

    @classmethod
    def validate_two_cases(cls, address, json_obj, other_json_obj):
        res = []
        res.extend(cls.validate_two_nested_jsons(address, json_obj, other_json_obj,
                                                 ignored_list=['summary', 'diagnoses', 'observation']))
        res.extend(cls.validate_two_list_jsons(address, json_obj['diagnoses'], other_json_obj['diagnoses'],
                                               ['diagnosis_id'], cls.validate_two_flat_jsons))
        res.extend(cls.validate_two_list_jsons(address, json_obj['summary']['data_categories'],
                                               other_json_obj['summary']['data_categories'],
                                               ['data_category', 'file_count']))
        res.extend(cls.validate_two_list_jsons(address, json_obj['observation'], other_json_obj['observation'],
                                               ['src_vcf_id'], cls.validate_two_flat_jsons))
        return res

    @classmethod
    def validate_two_transcripts(cls, address, json_obj, other_json_obj):
        return cls.validate_two_nested_jsons(address, json_obj, other_json_obj)
