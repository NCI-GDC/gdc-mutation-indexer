import unittest
import pytest
import json
from jsonpath_rw import parse

from conftest import get_validation_paths
from config import TestConfig

conf = TestConfig

@pytest.yield_fixture(scope='module')
def case_centric_index():
    ''' Generates a case centric index for testing '''
    es = Elasticsearch(conf.es_host, port=conf.es_port)

    r = es.indices.create(index=conf.indices['case_centric'], ignore=400)

    yield es
    if not conf.keep_indices:
        es.indices.delete(index=conf.indices['case_centric'], ignore=399)

### Test for field paths in the case document
@pytest.mark.parametrize('doc,path',
    get_validation_paths('tests/data/case.validation.1bf54408-b5cb-45dc-ad03-ef2866a0ff59.json')
)
def test_doc_contains(test_index, doc, path):
    ''' Test that document contains a field from a path'''
    d = test_index.get(conf.graph_index,
                             doc,
                             doc_type='case')
    d = d['_source']
    results = parse(path).find(d)
    assert len([r.value for r in results]) == 1
