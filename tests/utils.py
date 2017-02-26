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


class TestJsonObject(unittest.TestCase):
    def validate_transcript_list(self, list_transcripts, other_list_transcripts):
        self.validate_two_list_jsons(list_transcripts, other_list_transcripts, ["transcript_id"],
                                     self.validate_two_transcripts, "transcript")

    def validate_case_list(self, list_cases, other_list_cases):
        self.validate_two_list_jsons(list_cases, other_list_cases, ["submitter_id"],
                                     self.validate_two_cases, "case")

    def build_dict_from_list_json(self, list_json, identity_fields, object_name=None):
        res_dict = {}
        for js in list_json:
            key = self.identity_from_identity_fields(js, identity_fields, object_name)
            res_dict[key] = js
        return res_dict

    def identity_from_identity_fields(self, json, identity_fields, object_name=None):
        if object_name is not None:
            json = json[object_name]
        identity = ''
        for key in identity_fields:
            identity += json[key] + '-'
        identity = identity[:-1]
        return identity

    def validate_two_flat_lists(self, f_list, other_list):
        self.assertEqual(len(f_list), len(other_list), "two list don't have same size")
        s = set(f_list)
        for item in other_list:
            self.assertIn(item, other_list, "{0} does not exists in {1}".format(item, s))

    def validate_two_flat_jsons(self, json_obj, other_json_obj):
        for field in json_obj.keys():
            self.assertIn(field, other_json_obj.keys(),
                          "{0} does not exists in {1}".format(field, other_json_obj.keys()))
            self.assertEqual(json_obj[field], other_json_obj[field],
                             "{0} is not equal {1}".format(json_obj[field], other_json_obj[field]))

    def validate_two_list_jsons(self, list_jsons, other_list_jsons, identity_fields, diff_func=None, object_name=None):
        dict_jsons = self.build_dict_from_list_json(list_jsons, identity_fields, object_name)
        other_dict_jsons = self.build_dict_from_list_json(other_list_jsons, identity_fields, object_name)
        first_size = len(dict_jsons.keys())
        second_size = len(other_dict_jsons.keys())
        assert first_size == second_size

        for key in dict_jsons.keys():
            assert key in other_dict_jsons.keys()
            if diff_func:
                diff_func(dict_jsons[key], other_dict_jsons[key])

    def validate_two_nested_jsons(self, json_obj, other_json_obj, ignore_list=None):
        if ignore_list is None:
            ignore_list = []
        for field in json_obj.keys():
            assert field in other_json_obj.keys()
            if field not in ignore_list:
                if type(json_obj[field]) is list:
                    self.validate_two_flat_lists(json_obj[field], other_json_obj[field])
                elif type(json_obj[field]) is dict:
                    self.validate_two_nested_jsons(json_obj[field], other_json_obj[field])
                else:
                    self.assertEqual(json_obj[field], other_json_obj[field],
                                     "{0} is not equal {1}".format(json_obj[field], other_json_obj[field]))

    def validate_two_cases(self, json_obj, other_json_obj):
        self.validate_two_nested_jsons(json_obj, other_json_obj,
                                       ignore_list=['summary', 'diagnoses', 'observation'])
        self.validate_two_list_jsons(json_obj['diagnoses'], other_json_obj['diagnoses'],
                                     ['diagnosis_id'], self.validate_two_flat_jsons)
        self.validate_two_list_jsons(json_obj['summary']['data_categories'],
                                     other_json_obj['summary']['data_categories'],
                                     ['data_category', 'file_count'])
        self.validate_two_list_jsons(json_obj['observation'], other_json_obj['observation'],
                                     ['src_vcf_id'], self.validate_two_flat_jsons)

    def validate_two_transcripts(self, json_obj, other_json_obj):
        self.validate_two_nested_jsons(json_obj, other_json_obj)
