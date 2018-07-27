import time
import json
import pytest
import logging

from pyspark import SparkContext
from pyspark.sql import SQLContext

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tests_config import TestConfig
from cdisutils.dictionary import remove_keys_from_dict

from exports.builders.utils import get_case_ids_from_source_es
from exports.mappers.models_mapper import ModelMapper
from utils.maf_metrics import MAFStats
from utils.true_stats import TestDataStats
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
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
    print '\n\n\tSETTING UP TEST INDEX\n\n'
    es = Elasticsearch(conf.source_es_host, port=conf.es_port)

    case_mapping = ModelMapper('gdc_from_graph').create_index_settings()

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

    case_docs = remove_keys_from_dict(case_docs, ['file_state'])

    log.info('Bulk loading case docs to the ES...')
    bulk(es, case_docs['docs'], ignore=409)

    log.info('loaded {} case docs'.format(len(case_docs['docs'])))

    while True:
        count = es.count(index=conf.graph_index, doc_type='case')['count']
        print count, len(case_docs['docs'])
        if count >= len(case_docs['docs']):
            assert count == len(case_docs['docs'])
            break
        time.sleep(5)
    # Wait for index to be refreshed
    time.sleep(1)
    return es


@pytest.fixture(scope='session')
def es_client(setup_test_index):
    return setup_test_index


@pytest.fixture(scope='session')
def sqlContext(es_client):
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
def all_cases(sqlContext, maf_df):
    """
    Returns all case_ids expected to build, including "empty cases"
    The info is taken from aliquots in test maf headers
    """
    cases = get_case_ids_from_source_es(conf, sqlContext, conf.maf_urls)
    return [json.loads(c)['case_id'] for c in cases.toJSON().collect()]


@pytest.fixture(scope='session')
def test_data():
    return TestDataStats.load_test_data(conf.input_dir)


@pytest.fixture(scope="session")
def maf_df(sqlContext):
    """
    Builds combined maf dataframe once. Reused throughout test suite
    """
    log.info('\n\n\tBUILDING MAF_DF\n\n')
    return MAFBuilder(conf, sqlContext).build()


@pytest.fixture(scope='session')
def case_df(sqlContext, maf_df):
    return CaseBuilder(conf, sqlContext).build(maf_df)


@pytest.fixture(scope='session')
def ssm_transcript_df(sqlContext, maf_df):
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    log.info('\n\n\tBUILDING SSM_TRANSCRIPT_DF\n\n')
    return ConsequenceBuilder(conf, sqlContext).build_all_effects_cols(maf_df)


@pytest.fixture(scope='session')
def case_centric_df(sqlContext, maf_df):
    """
    Builds case centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING CASE_CENTRIC_DF\n\n')
    builder = CaseCentricBuilder(conf, sqlContext)
    builder.build(maf_df)

    log.info('\n\n\tLOADING CASE_CENTRIC_DF\n\n')
    builder.load()
    return builder.case_centric


@pytest.fixture(scope='session')
def gene_centric_df(sqlContext, maf_df):
    """
    Builds gene centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING GENE_CENTRIC_DF\n\n')
    builder = GeneCentricBuilder(conf, sqlContext)
    builder.build(maf_df)

    log.info('\n\n\tLOADING GENE_CENTRIC_DF\n\n')
    builder.load()
    return builder.gene_centric


@pytest.fixture(scope='session')
def ssm_centric_df(sqlContext, maf_df):
    """
    Builds ssm centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_CENTRIC_DF\n\n')
    builder = SSMCentricBuilder(conf, sqlContext)
    builder.build(maf_df)

    log.info('\n\n\tLOADING SSM_CENTRIC_DF\n\n')
    builder.load()
    return builder.ssm_centric


@pytest.fixture(scope='session')
def ssm_occurrence_centric_df(sqlContext, maf_df):
    """
    Builds ssm occurrence centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    builder.build(maf_df)

    log.info('\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    return builder.ssm_occurrence_centric


@pytest.fixture(scope='session')
def case_ssm_subtree(sqlContext, maf_df):
    """
    Builds case centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING CASE_SSM_SUBTREE\n\n')
    builder = CaseCentricBuilder(conf, sqlContext)
    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope='session')
def gene_ssm_subtree(sqlContext, maf_df):
    """
    Builds gene centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING GENE_SSM_SUBTREE\n\n')
    builder = GeneCentricBuilder(conf, sqlContext)
    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope='session')
def ssm_occurrence_ssm_subtree(sqlContext, maf_df):
    """
    Builds ssm occurrence centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING SSM_OCCURRENCE_SSM_SUBTREE\n\n')
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope='module')
def maf_stats():
    yield MAFStats(conf.maf_urls)
