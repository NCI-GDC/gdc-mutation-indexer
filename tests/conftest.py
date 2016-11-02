import os
import time
import json
import pytest

from elasticsearch import Elasticsearch


TEST_DIR = os.path.dirname(os.path.realpath(__file__))

ES_HOST = 'localhost'
ES_PORT = 9200
ES_INDEX = 'test_graph_index__'

@pytest.yield_fixture(scope='class')
def test_index(request):
    """Generate a graph index as a fixture for re-use between tests"""
    request.cls.es = Elasticsearch(ES_HOST, port=ES_PORT)

    r = request.cls.es.indices.create(index=ES_INDEX, ignore=400)
    request.cls.graph_index = ES_INDEX

    with open(os.path.join(TEST_DIR,'data','cases.json')) as f:
        case_docs = json.load(f)

    for doc in case_docs['docs']:
        request.cls.es.create(
            index=ES_INDEX,
            id=doc['_id'],
            doc_type=doc['_type'],
            body=doc['_source'],
            ignore=409,
        )

    while True:
        count = request.cls.es.count(index=ES_INDEX, doc_type='case')['count']
        if count == len(case_docs):
            break
        time.sleep(0.1)

    yield request.cls.es
    request.cls.es.indices.delete(index=ES_INDEX, ignore=399)
