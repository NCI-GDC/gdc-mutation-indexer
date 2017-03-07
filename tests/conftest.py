import gzip
import time
import json
import pytest
import logging

from pyspark import SparkContext
from pyspark.sql import SQLContext

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tests_config import TestConfig

from exports.builders import MAFBuilder
from utils.maf_metrics import MAFStats

conf = TestConfig()

log = logging.getLogger()
log.setLevel(logging.INFO)


@pytest.fixture(scope='session')
def setup_test_index():
    '''
    Creates graph index with case docs and returns an elasticsearch client
    '''
    es = Elasticsearch(conf.source_es_host, port=conf.es_port)

    with open(conf.case_mapping_json, 'r') as f:
        case_mapping = json.load(f)

    if es.indices.exists(conf.graph_index):
        if not conf.graph_force_build:
            return es
        es.indices.delete(index=conf.graph_index)

    es.indices.create(index=conf.graph_index, ignore=400, body=case_mapping)

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
                         '_source': doc}
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


@pytest.fixture(scope='session')
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


@pytest.fixture(scope="session")
def maf_df(sqlContext):
    print "\n\n\tBUILDING MAF\n\n"
    yield MAFBuilder(conf, sqlContext).build()


@pytest.fixture(scope='class')
def test_index_class(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    request.cls.es = setup_test_index()
    request.cls.config = conf

    yield request.cls.es

    if not conf.keep_indices:
        request.cls.es.indices.delete(index=conf.graph_index, ignore=399)


@pytest.fixture(scope='module')
def test_index(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    es = setup_test_index()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.graph_index, ignore=399)


@pytest.fixture(scope='module')
def maf_stats():
    yield MAFStats(conf.maf_urls)
