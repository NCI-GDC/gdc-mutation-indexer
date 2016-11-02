import os
import time
import json
import pytest

from elasticsearch import Elasticsearch


TEST_DIR = os.path.dirname(os.path.realpath(__file__))

ES_HOST = 'localhost'
ES_PORT = 9200
ES_INDEX = 'test_graph_index__'

@pytest.yield_fixture(scope='module')
def test_index():
    """Generate an index as a fixture for re-use between tests"""

    es_driver = Elasticsearch(ES_HOST, port=ES_PORT)

    r = es_driver.indices.create(index=ES_INDEX, ignore=400)

    with open(os.path.join(TEST_DIR,'data','cases.json')) as f:
        case_docs = json.load(f)

    for doc in case_docs['docs']:
        es_driver.create(
            index=ES_INDEX,
            id=doc['_id'],
            doc_type=doc['_type'],
            body=doc['_source'],
            ignore=409,
        )

    while True:
        count = es_driver.count(index=ES_INDEX, doc_type='case')['count']
        if count == len(case_docs):
            break
        time.sleep(0.1)

    yield es_driver, ES_INDEX, 'case', case_docs
    es_driver.indices.delete(index=ES_INDEX, ignore=399)
