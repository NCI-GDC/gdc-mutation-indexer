import os
import time
import json
import pytest

from elasticsearch import Elasticsearch
from config import TestConfig

conf = TestConfig


def setup_test_index():
    '''
    Creates graph index with case docs and returns an elasticsearch client
    '''
    es = Elasticsearch(conf.source_es_host, port=conf.es_port)
    r = es.indices.create(index=conf.graph_index, ignore=400)

    with open(os.path.join(conf.data_dir, 'cases.json')) as f:
        case_docs = json.load(f)

    for doc in case_docs['docs']:
        es.create(
            index=conf.graph_index,
            id=doc['_id'],
            doc_type=doc['_type'],
            body=doc['_source'],
            ignore=409,
        )

    print 'loaded {} case docs'.format(len(case_docs['docs']))

    while True:
        count = es.count(index=conf.graph_index, doc_type='case')['count']
        if count >= len(case_docs):
            break
        time.sleep(0.1)

    return es

@pytest.yield_fixture(scope='class')
def test_index_class(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    request.cls.es = setup_test_index()
    request.cls.config = conf

    yield request.cls.es

    if not conf.keep_indices:
        request.cls.es.indices.delete(index=conf.graph_index, ignore=399)

@pytest.yield_fixture(scope='module')
def test_index(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    es = setup_test_index()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.graph_index, ignore=399)
