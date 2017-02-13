import os
import pytest
from deepdiff import DeepDiff

from config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import CaseCentricBuilder

builder = CaseCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def case_centric_index(sqlContext, test_index):
    """ Generates a case centric index for testing """
    es = T.generate_index(sqlContext)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)

@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_formal(case_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)

    diff = DeepDiff(es_doc, true_doc,
                    ignore_order=True, view='tree')
    T.report_deepdiff(diff)
    assert diff == {}


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_flat(case_centric_index, filename):
    es_doc, true_doc = map(T.flatten_json,
                           T.get_docs_to_compare(case_centric_index, filename))

    T.report_correctness(es_doc, true_doc, label=filename)

    for k, v in true_doc.items():
        assert k in es_doc
        assert es_doc[k] == v

