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


class TestJsonObject(object):
    @classmethod
    def validate_transcript_list(cls, list_transcripts, other_list_transcripts):
        cls.validate_two_list_jsons(list_transcripts, other_list_transcripts, ["transcript_id"],
                                    cls.validate_two_transcripts, "transcript")

    @classmethod
    def validate_case_list(cls, list_cases, other_list_cases):
        cls.validate_two_list_jsons(list_cases, other_list_cases, ["submitter_id"],
                                    cls.validate_two_cases, "case")

    @classmethod
    def build_dict_from_list_json(cls, list_json, identity_fields, object_name=None):
        res_dict = {}
        for js in list_json:
            key = cls.identity_from_identity_fields(js, identity_fields, object_name)
            res_dict[key] = js
        return res_dict

    @classmethod
    def identity_from_identity_fields(cls, json, identity_fields, object_name=None):
        if object_name is not None:
            json = json[object_name]
        identity = ''
        for key in identity_fields:
            identity += json[key] + '-'
        identity = identity[:-1]
        return identity

    @classmethod
    def validate_two_flat_lists(self, f_list, other_list):
        assert len(f_list) == len(other_list)
        s = set(f_list)
        for item in other_list:
            assert item in s

    @classmethod
    def validate_two_flat_jsons(self, json_obj, other_json_obj):
        for field in json_obj.keys():
            assert field in other_json_obj.keys()
            assert json_obj[field] == json_obj[field]

    @classmethod
    def validate_two_list_jsons(cls, list_jsons, other_list_jsons, identity_fields, diff_func=None, object_name=None):
        dict_jsons = cls.build_dict_from_list_json(list_jsons, identity_fields, object_name)
        other_dict_jsons = cls.build_dict_from_list_json(other_list_jsons, identity_fields, object_name)
        first_size = len(dict_jsons.keys())
        second_size = len(other_dict_jsons.keys())
        assert first_size == second_size

        for key in dict_jsons.keys():
            assert key in other_dict_jsons.keys()
            if diff_func:
                diff_func(dict_jsons[key], other_dict_jsons[key])

    @classmethod
    def validate_two_nested_jsons(cls, json_obj, other_json_obj, ignore_list=None):
        if ignore_list is None:
            ignore_list = []
        for field in json_obj.keys():
            assert field in other_json_obj.keys()
            if field not in ignore_list:
                if json_obj[field] is list:
                    cls.validate_two_flat_lists(json_obj[field], other_json_obj[field])
                elif json_obj[field] is dict:
                    cls.validate_two_nested_jsons(json_obj[field], other_json_obj[field])
                else:
                    assert json_obj[field] == other_json_obj[field]

    @classmethod
    def validate_two_cases(cls, json_obj, other_json_obj):
        cls.validate_two_nested_jsons(json_obj, other_json_obj,
                                      ignore_list=['summary', 'diagnoses', 'observation'])
        cls.validate_two_list_jsons(json_obj['diagnoses'], other_json_obj['diagnoses'],
                                    ['diagnosis_id'], cls.validate_two_flat_jsons)
        cls.validate_two_list_jsons(json_obj['summary']['data_categories'],
                                    other_json_obj['summary']['data_categories'],
                                    ['data_category', 'file_count'])
        cls.validate_two_list_jsons(json_obj['observation'], other_json_obj['observation'],
                                    ['src_vcf_id'], cls.validate_two_flat_jsons)

    @classmethod
    def validate_two_transcripts(cls, json_obj, other_json_obj):
        cls.validate_two_nested_jsons(json_obj, other_json_obj)
