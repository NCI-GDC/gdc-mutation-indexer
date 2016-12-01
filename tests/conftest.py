import os
import time
import json
import pytest

from elasticsearch import Elasticsearch
from config import TestConfig

conf = TestConfig

@pytest.yield_fixture(scope='class')
def test_index(request):
    """Generate a graph index as a fixture for re-use between tests"""
    request.cls.es = Elasticsearch(conf.es_host, port=conf.es_port)

    r = request.cls.es.indices.create(index=conf.graph_index, ignore=400)
    request.cls.graph_index = conf.graph_index

    with open(os.path.join(conf.data_dir, 'cases.json')) as f:
        case_docs = json.load(f)

    for doc in case_docs['docs']:
        request.cls.es.create(
            index=conf.graph_index,
            id=doc['_id'],
            doc_type=doc['_type'],
            body=doc['_source'],
            ignore=409,
        )

    while True:
        count = request.cls.es.count(index=conf.graph_index, doc_type='case')['count']
        if count == len(case_docs):
            break
        time.sleep(0.1)

    yield request.cls.es
    request.cls.es.indices.delete(index=conf.graph_index, ignore=399)
