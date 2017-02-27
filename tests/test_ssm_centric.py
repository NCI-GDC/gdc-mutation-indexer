import os
import pytest
from deepdiff import DeepDiff

from config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMCentricBuilder
<<<<<<< 6bb5f0ed40b84956f4e9042a606e4f0f4307fe1b
from utils import JSONValidator
=======
>>>>>>> fix(ssm): make unittest work

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
def test_ssm_centric_formal(ssm_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)

    diff = DeepDiff(es_doc, true_doc,
                    ignore_order=True, view='tree')
    T.report_deepdiff(diff)
    assert diff == {}


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_centric_flat(ssm_centric_index, filename):
    es_doc, true_doc = map(T.flatten_json,
                           T.get_docs_to_compare(ssm_centric_index, filename))

    T.report_correctness(es_doc, true_doc, label=filename)

    for k, v in true_doc.items():
        assert k in es_doc
        assert es_doc[k] == v


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
@pytest.mark.parametrize('test_mode', ['list', 'dict'])
def test_ssm_centric_cardinality(ssm_centric_index, filename, test_mode):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
    mismatches = JSONValidator.find_mismatches(es_doc, true_doc, test_mode)
    T.report_cardinality(mismatches, '[{}|{}]'.format(filename, test_mode))
    assert mismatches == {}
