import os
import pytest

from config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMCentricBuilder
from utils import TestJsonObject, KEY_VALUE_SEPARATOR

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

#
# @pytest.mark.parametrize('filename', os.listdir(T.output_dir))
# def test_ssm_centric_deep(ssm_centric_index, filename):
#     es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
#     diffs = TestJsonObject.validate_transcript_list("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                                     es_doc['consequence'], true_doc['consequence'])
#     diffs.extend(TestJsonObject.validate_case_list("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                                    es_doc['occurrence'], true_doc['occurrence']))
#     diffs.extend(TestJsonObject.validate_two_nested_jsons("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
#                                                           es_doc, true_doc, ignored_list=['consequence', 'occurrence']))
#     assert diffs == []
