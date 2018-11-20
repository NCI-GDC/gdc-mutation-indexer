import time
import pytest
import logging

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tests_config import TestConfig
from cdisutils.dictionary import remove_keys_from_dict

from exports.builders.utils import (
    get_case_ids_from_source_es,
    iterate_es_results,
)
from exports.mappers.distinct_doctype_model_mapper import (
    DistinctDocTypeModelMapper
)
from utils.maf_metrics import MAFStats
from utils.true_stats import TestDataStats
from exports.builders import (
    MAFBuilder,
    GisticBuilder,
    CaseBuilder,
    CNVCentricBuilder,
    CNVOccurrenceCentricBuilder,
    ConsequenceBuilder,
    GeneCentricBuilder,
    CaseCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder,
)

conf = TestConfig()

log = logging.getLogger()
log.setLevel(logging.INFO)


@pytest.fixture(scope='session')
def setup_test_index():
    """
    Creates graph index with required docs and returns an elasticsearch client
    """
    print '\n\n\tSETTING UP TEST INDEX\n\n'
    es = Elasticsearch(conf.source_es_host, port=conf.es_port)

    # if index already exists and we don't need to force rebuild,
    # return existing index
    if es.indices.exists(conf.graph_index):
        if not conf.graph_force_build:
            return es
        es.indices.delete(index=conf.graph_index)

    # set up test ES index
    create_test_index(es)

    # insert documents
    load_docs_into_test_index(es, 'case')
    load_docs_into_test_index(es, 'file')

    return es


def create_test_index(es):
    """
    Creating an index in elasticsearch requires all doc_type mapping
    and settings upfront.
    """
    case_model_mapper = DistinctDocTypeModelMapper('gdc_from_graph',
                                                   'case')

    file_model_mapper = DistinctDocTypeModelMapper('gdc_from_graph',
                                                   'file')

    combined = {'mappings': {}, 'settings': {}}
    combined['mappings'].update(case_model_mapper.index_settings['mappings']) 
    combined['mappings'].update(file_model_mapper.index_settings['mappings'])

    combined['settings'].update(case_model_mapper.index_settings['settings'])
    combined['settings'].update(file_model_mapper.index_settings['settings'])

    # set up index/doc_type
    es.indices.create(index=conf.graph_index,
                      ignore=400,
                      body=combined)


def load_docs_into_test_index(es, doc_type):
    """
    Load documents from zipped test data into test index.
    """

    docs = {'docs': []}
    for doc in TestDataStats.load_es_graph_dump(conf.doc_files[doc_type]):
        to_append = {'_id': doc['{}_id'.format(doc_type)],
                     '_index': conf.graph_index,
                     '_type': doc_type,
                     '_source': doc}
        docs['docs'].append(to_append)

    # Remove .cases[] from underneath case.files[]
    if doc_type == 'case':
        for doc in docs['docs']:
            for _file in doc['_source']['files']:
                _file.pop('cases', None)

    # TODO: temp fix
    docs = remove_keys_from_dict(docs, ['file_state'])

    log.info('Bulk loading {} docs to the ES...'.format(doc_type))
    bulk(es, docs['docs'], ignore=409)

    log.info('loaded {} {} docs'.format(len(docs['docs']), doc_type))

    es.indices.refresh(index=conf.graph_index)


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
def all_maf_cases(sqlContext, maf_df):
    """
    Returns all case_ids expected to build and have 'ssm' in available_variation_data
    (including "empty cases" - ones that have been tested for ssm but had none)
    The info is taken from aliquots in test maf headers
    """
    # Read aliquots from maf headers and get list of corresponding cases:
    cases = get_case_ids_from_source_es(conf, sqlContext, conf.maf_urls)
    return {c.case_id for c in cases.collect()}


@pytest.fixture(scope='session')
def all_cases(es_client):
    """
    Returns the IDs of all cases in the GDC graph, including those with no
    maf or cnv data
    """
    hits = iterate_es_results(es_client=es_client,
                              index_name=conf.graph_index,
                              doc_type=conf.graph_document,
                              query={'_source': ['case_id']})

    return {hit['_source']['case_id'] for hit in hits}


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


@pytest.fixture(scope="session")
def acl_maf_df(sqlContext, maf_df):
    """
    Builds combined maf dataframe
    Note: alters naturally-occurring acls for testing purposes.
    """
    def fake_out_acl(chromosome):
        if int(chromosome) % 2 == 0:
            return [u'phs000218']
        return [u'open']

    acl_udf = udf(fake_out_acl, ArrayType(StringType()))
    altered_maf = maf_df.drop('acl')
    altered_maf = altered_maf.withColumn('acl',
                                         acl_udf('gene_chromosome'))

    return altered_maf


@pytest.fixture(scope="session")
def gistic_df(sqlContext):
    """
    Builds combined gistic dataframe once. Reused throughout test suite
    """
    log.info('\n\n\tBUILDING GISTIC_DF\n\n')
    return GisticBuilder(conf, sqlContext).build()


@pytest.fixture(scope='session')
def case_df(sqlContext, maf_df, gistic_df):
    return CaseBuilder(conf, sqlContext).build(maf_df, gistic_df)


@pytest.fixture(scope='session')
def ssm_transcript_df(sqlContext, maf_df):
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    log.info('\n\n\tBUILDING SSM_TRANSCRIPT_DF\n\n')
    return ConsequenceBuilder(conf, sqlContext).build_all_effects_cols(maf_df)


@pytest.fixture(scope='session')
def case_centric_df(sqlContext, maf_df, gistic_df, case_df):
    """
    Builds case centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING CASE_CENTRIC_DF\n\n')
    builder = CaseCentricBuilder(conf, sqlContext)
    builder.build(maf_df, gistic_df, case_df)

    log.info('\n\n\tLOADING CASE_CENTRIC_DF\n\n')
    builder.load()
    return builder.case_centric


@pytest.fixture(scope='session')
def gene_centric_df(sqlContext, maf_df, gistic_df, case_df):
    """
    Builds gene centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING GENE_CENTRIC_DF\n\n')
    builder = GeneCentricBuilder(conf, sqlContext)
    builder.build(maf_df, gistic_df, case_df)

    log.info('\n\n\tLOADING GENE_CENTRIC_DF\n\n')
    builder.load()
    return builder.gene_centric


@pytest.fixture(scope='session')
def ssm_centric_df(sqlContext, maf_df, case_df):
    """
    Builds ssm centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_CENTRIC_DF\n\n')
    builder = SSMCentricBuilder(conf, sqlContext)
    builder.build(maf_df, case_df)

    log.info('\n\n\tLOADING SSM_CENTRIC_DF\n\n')
    builder.load()
    return builder.ssm_centric


@pytest.fixture(scope='session')
def ssm_occurrence_centric_df(sqlContext, maf_df, case_df):
    """
    Builds ssm occurrence centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    builder.build(maf_df, case_df)

    log.info('\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    return builder.ssm_occurrence_centric


@pytest.fixture(scope='session')
def cnv_centric_df(sqlContext, gistic_df, case_df):
    """
    Builds cnv centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_CENTRIC DF\n\n')
    builder = CNVCentricBuilder(conf, sqlContext)
    builder.build(gistic_df, case_df)

    log.info('\n\n\tLOADING CNV_CENTRIC_DF\n\n')
    builder.load()
    return builder.cnv_centric


@pytest.fixture(scope='session')
def cnv_occurrence_centric_df(sqlContext, gistic_df, case_df):
    """
    Builds cnv occurrence centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_OCCURRENCE_CENTRIC DF\n\n')
    builder = CNVOccurrenceCentricBuilder(conf, sqlContext)
    builder.build(gistic_df, case_df)

    log.info('\n\n\tLOADING CNV_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    return builder.cnv_occurrence_centric


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

