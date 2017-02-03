import os
import time
import json
import pytest

from elasticsearch import Elasticsearch
from config import TestConfig

conf = TestConfig

log = logging.getLogger()
log.setLevel(logging.INFO)


def setup_test_index():
    '''
    Creates graph index with case docs and returns an elasticsearch client
    '''

    es = Elasticsearch(conf.source_es_host, port=conf.es_port)

    with open(os.path.join(conf.data_dir, 'case_mapping.json')) as f:
        case_mapping = json.load(f)

    r = request.cls.es.indices.create(index=conf.graph_index, ignore=400)
    request.cls.graph_index = conf.graph_index

    try:
        with open(conf.cases_file) as f:
            case_docs = json.load(f)
    except:
        case_docs = {'docs': []}
        with open(conf.cases_file) as f:
            for line in f.readlines():
                doc = json.loads(line)
                to_append = {'_id': doc['case_id'],
                             '_type': 'case',
                             '_source': {k: v for k, v in doc.items()
                                         if k != 'case_id'}}
                case_docs['docs'].append(to_append)


    for doc in case_docs['docs']:
        i += 1
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

    if not conf.keep_indices:
        request.cls.es.indices.delete(index=conf.graph_index, ignore=399)


@pytest.yield_fixture(scope='module')
def test_index(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    es = setup_test_index()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.graph_index, ignore=399)

### Validation helpers

def get_validation_doc(path):
    '''
    Loads a json document for validation and flattens it to a dict
    '''
    with open(path) as f:
        validation = json.load(f)['_source']

    paths = {}

    def get_fields(doc, name=''):
        if type(doc) is dict:
            for k,v in doc.items():
                get_fields(v, name + '.' + k)
        elif type(doc) is list:
            for v in doc:
                get_fields(v, name + '[*]')
        else:
            paths[name[1:]] = doc

    get_fields(validation)
    return paths


def get_validation_paths(path):
    '''
    Gets the field paths from a json file and sorts them by length for
    nice traceback during testing
    '''
    did = path.split('.')[-2]
    fields = get_validation_doc(path)
    return zip([did]*len(fields.keys()),
               sorted(fields.keys(), key=lambda x: len(x)))
