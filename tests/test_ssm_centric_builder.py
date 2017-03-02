import os
import pytest

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMCentricBuilder
from json_test_utils import (validate_two_nested_jsons,
                             KEY_VALUE_SEPARATOR)

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
    diffs = validate_two_nested_jsons("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc['consequence'], true_doc['consequence'])
    assert diffs == []


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_centric_in_depth(ssm_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
    diffs = validate_two_nested_jsons("ssm{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc['consequence'], true_doc['consequence'], True)
    assert diffs == []
