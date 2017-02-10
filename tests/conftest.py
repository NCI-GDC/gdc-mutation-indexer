import os
import gzip
import time
import json
import pytest
import logging

from pyspark import SparkContext
from pyspark.sql import SQLContext

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
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

    if es.indices.exists(conf.graph_index):
        if not conf.graph_force_build:
            return es
        es.indices.delete(index=conf.graph_index)

    r = es.indices.create(index=conf.graph_index, ignore=400, body=case_mapping)

    if conf.cases_file.endswith('.gz'):
        f = gzip.open(conf.cases_file, 'rb')
    else:
        f = open(conf.cases_file, 'rb')

    try:
        case_docs = json.load(f)
    except:
        f.seek(0)
        # If instead the file is a case doc per line
        case_docs = {'docs': []}
        for line in f.readlines():
            doc = json.loads(line)
            to_append = {'_id': doc['case_id'],
                         '_index': conf.graph_index,
                         '_type': 'case',
                         '_source': {k: v for k, v in doc.items()}}
            case_docs['docs'].append(to_append)

    log.info('Bulk loading case docs to the ES...')
    bulk(es, case_docs['docs'], ignore=409)

    log.info('loaded {} case docs'.format(len(case_docs['docs'])))

    while True:
        count = es.count(index=conf.graph_index, doc_type='case')['count']
        if count >= len(case_docs):
            break
        time.sleep(0.1)

    return es


@pytest.yield_fixture(scope='module')
def sqlContext():
    sc = SparkContext(conf.spark_master, 'sqlContextFixture')
    sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
    sqlCont = SQLContext(sc)
    sqlCont.sql("set spark.sql.shuffle.partitions=200")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    yield sqlCont

    sc.stop()
    sc._jvm.System.clearProperty("spark.driver.port")


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
