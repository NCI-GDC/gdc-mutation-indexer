import unittest
import pytest
from jsonpath_rw import parse

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

@pytest.mark.parametrize('doc,path', [
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59','case_id')
])
def test_doc_contains(test_index, doc, path):
    
    d = test_index.get(conf.graph_index,
                             doc,
                             doc_type='case')
    results = parse(path).find(d)
    assert len([r.value for r in results]) == 0
