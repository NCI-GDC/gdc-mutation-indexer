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

from exports.mappers.models_mapper import ModelMapper
from utils.maf_metrics import MAFStats
from utils.true_stats import TestDataStats
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ObservationBuilder,
    ConsequenceBuilder,
    GeneCentricBuilder,
    CaseCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)

conf = TestConfig()

log = logging.getLogger()
log.setLevel(logging.INFO)


@pytest.fixture(scope='session')
def setup_test_index():
    """
    Creates graph index with case docs and returns an elasticsearch client
    """
    es = Elasticsearch(conf.source_es_host, port=conf.es_port)

    case_mapping = ModelMapper('case').create_index_settings()

    if es.indices.exists(conf.graph_index):
        if not conf.graph_force_build:
            return es
        es.indices.delete(index=conf.graph_index)

    es.indices.create(index=conf.graph_index, ignore=400, body=case_mapping)

    case_docs = {'docs': []}
    for case_doc in TestDataStats.load_es_graph_dump(conf.cases_file):
        to_append = {'_id': case_doc['case_id'],
                     '_index': conf.graph_index,
                     '_type': 'case',
                     '_source': case_doc}
        case_docs['docs'].append(to_append)

    # Remove .cases[] from case.files[].cases[]
    for case in case_docs['docs']:
        for _file in case['_source']['files']:
            _file.pop('cases', None)

    log.info('Bulk loading case docs to the ES...')
    bulk(es, case_docs['docs'], ignore=409)

    log.info('loaded {} case docs'.format(len(case_docs['docs'])))

    while True:
        count = es.count(index=conf.graph_index, doc_type='case')['count']
        if count >= len(case_docs):
            break
        time.sleep(0.1)
    # Wait for index to be refreshed
    time.sleep(1.0)
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


@pytest.fixture(scope='session')
def test_data():
    return TestDataStats.load_test_data(conf.input_dir)


@pytest.fixture(scope="session")
def maf_df(sqlContext):
    """
    Builds combined maf dataframe once. Reused throughout test suite
    """
    yield MAFBuilder(conf, sqlContext).build()


@pytest.fixture(scope='session')
def ssm_transcript_df(sqlContext, maf_df):
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    return ConsequenceBuilder(conf, sqlContext)._build_all_effects_cols(maf_df)


@pytest.fixture(scope='session')
def case_centric_df(sqlContext, maf_df):
    """
    Builds case centric dataframe once. Reused throughout test suite
    """
    builder = CaseCentricBuilder(conf, sqlContext)
    yield builder.build(maf_df).case_centric


@pytest.fixture(scope='session')
def gene_centric_df(sqlContext, maf_df):
    """
    Builds gene centric dataframe once. Reused throughout test suite
    """
    builder = GeneCentricBuilder(conf, sqlContext)
    yield builder.build(maf_df).gene_centric


@pytest.fixture(scope='session')
def ssm_centric_df(sqlContext, maf_df):
    """
    Builds ssm centric dataframe once. Reused throughout test suite
    """
    builder = SSMCentricBuilder(conf, sqlContext)
    yield builder.build(maf_df).ssm_centric


@pytest.fixture(scope='session')
def ssm_occurrence_centric_df(sqlContext, maf_df):
    """
    Builds ssm occurrence centric dataframe once. Reused throughout test suite
    """
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    yield builder.build(maf_df).ssm_occurrence_centric


@pytest.fixture(scope='session')
def case_centric_index(sqlContext, case_centric_df):
    """
    Generates case centric index for testing
    Does not rebuild the dataframe, uses already built one
    """
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    builder = CaseCentricBuilder(conf, sqlContext)
    builder.case_centric = case_centric_df
    builder.load()

    yield es

    if not conf.keep_centric_indices:
        es.indices.delete(index=conf.indices[builder.index_name], ignore=399)


@pytest.fixture(scope='session')
def gene_centric_index(sqlContext, gene_centric_df):
    """
    Generates gene centric index for testing
    Does not rebuild the dataframe, uses already built one
    """
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    builder = GeneCentricBuilder(conf, sqlContext)
    builder.gene_centric = gene_centric_df
    builder.load()

    yield es

    if not conf.keep_centric_indices:
        es.indices.delete(index=conf.indices[builder.index_name], ignore=399)


@pytest.fixture(scope='session')
def ssm_centric_index(sqlContext, ssm_centric_df):
    """
    Generates ssm centric index for testing
    Does not rebuild the dataframe, uses already built one
    """
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    builder = SSMCentricBuilder(conf, sqlContext)
    builder.ssm_centric = ssm_centric_df
    builder.load()

    yield es

    if not conf.keep_centric_indices:
        es.indices.delete(index=conf.indices[builder.index_name], ignore=399)


@pytest.fixture(scope='session')
def ssm_occurrence_centric_index(sqlContext, ssm_occurrence_centric_df):
    """
    Generates ssm occurrence centric index for testing
    Does not rebuild the dataframe, uses already built one
    """
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    builder.ssm_occurrence_centric = ssm_occurrence_centric_df
    builder.load()

    yield es

    if not conf.keep_centric_indices:
        es.indices.delete(index=conf.indices[builder.index_name], ignore=399)


@pytest.fixture(scope='class')
def test_index_class(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    request.cls.es = setup_test_index()
    request.cls.config = conf

    yield request.cls.es

    if not conf.keep_graph_index:
        request.cls.es.indices.delete(index=conf.graph_index, ignore=399)


@pytest.fixture(scope='module')
def test_index(request):
    ''' Generate a graph index as a fixture for re-use between tests '''
    es = setup_test_index()

    yield es

    if not conf.keep_graph_index:
        es.indices.delete(index=conf.graph_index, ignore=399)


@pytest.fixture(scope='module')
def maf_stats():
    yield MAFStats(conf.maf_urls)
