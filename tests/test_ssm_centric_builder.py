import os
import pytest

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMCentricBuilder
from json_test_utils import (validate_two_nested_jsons,
                             validate_two_list_jsons,
                             validate_two_flat_jsons,
                             KEY_VALUE_SEPARATOR)
from df_test_utils import validate_consequence_list_in_dept, validate_consequence_join

builder = SSMCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def ssm_centric_index(sqlContext, test_index):
    """ Generates a ssm centric index for testing """
    es = T.generate_index(sqlContext)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_centric_join(ssm_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
    diffs = validate_consequence_join("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc['consequence'], true_doc['consequence'])
    diffs.extend(validate_two_list_jsons(address="ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                         list_jsons=es_doc['occurrence'],
                                         other_list_jsons=true_doc['occurrence'],
                                         identity_fields=["submitter_id"],
                                         object_name="case",
                                         diff_func=validate_two_occurrence, join_only=True))
    assert diffs == []

#
# @pytest.mark.parametrize('filename', os.listdir(T.output_dir))
# def test_ssm_centric_in_depth(ssm_centric_index, filename):
#     es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
#     diffs = validate_consequence_list_in_dept("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                               es_doc['consequence'], true_doc['consequence'])
#     diffs.extend(validate_two_list_jsons(address="ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                          list_jsons=es_doc['occurrence'],
#                                          other_list_jsons=true_doc['occurrence'],
#                                          identity_fields=["submitter_id"],
#                                          object_name="case",
#                                          diff_func=validate_two_occurrence))
#     diffs.extend(validate_two_nested_jsons("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                            es_doc, true_doc, ignored_list=['consequence', 'occurrence']))
#     assert diffs == []


def validate_two_occurrence(address, json_obj, other_json_obj):
    res = []
    res.extend(validate_two_nested_jsons(address, json_obj, other_json_obj,
                                         ignored_list=['summary', 'diagnoses', 'observation']))
    res.extend(validate_two_list_jsons(address, json_obj['diagnoses'], other_json_obj['diagnoses'],
                                       ['diagnosis_id'], diff_func=validate_two_flat_jsons))
    res.extend(validate_two_list_jsons(address, json_obj['summary']['data_categories'],
                                       other_json_obj['summary']['data_categories'],
                                       ['data_category', 'file_count']))
    res.extend(validate_two_list_jsons(address, json_obj['observation'], other_json_obj['observation'],
                                       ['src_vcf_id'], diff_func=validate_two_flat_jsons))
    return res
