import collections
import logging
import os

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest

from cdisutils.dictionary import remove_keys_from_dict
from elasticsearch.helpers import bulk
from normalizer.mapper import ModelMapper
from tests_config import TestConfig

from exports.builders.utils import (
    get_case_ids_from_source_es,
)
from exports.es_utils import (
    iterate_es_results,
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


GraphDocType = collections.namedtuple('GraphDocType', ['doc_type', 'model_name'])


GRAPH_INDICES = [GraphDocType('case', 'graph_case'), GraphDocType('file', 'graph_file')]


@pytest.fixture(scope='session')
def setup_graph_indices():
    """Create graph indices with required docs."""
    es = conf.es
    for doc_type, model_name in GRAPH_INDICES:
        print('\n\n\tSETTING UP {} TEST INDICES\n\n'.format(doc_type.upper()))
        if create_test_index(es, doc_type=doc_type, model_name=model_name):
            load_docs_into_test_index(es, doc_type)


def create_test_index(es, doc_type, model_name):
    """Create and configure an Elasticsearch index if needed.

    Skip creation if the index already exists, unless ``graph_force_build`` is set,
    on the assumption that we already populated the test data.

    Returns:
        True if the index was created; False if an existing index was reused.
    """
    index_name = conf.graph_indices[doc_type]
    if es.indices.exists(index_name):
        if not conf.graph_force_build:
            print('SKIPPING {} TEST INDEX SETUP'.format(doc_type.upper()))
            return False

        es.indices.delete(index_name)
        es.indices.refresh()

    # TODO Make sure this is how model mapper is actually gonna work.
    model_mapper = ModelMapper('gdc_from_graph', doc_type)
    es.indices.create(index=index_name, body=model_mapper.index_settings)

    return True


def load_docs_into_test_index(es, doc_type, input_path=None):
    """Load documents from gzipped test data into test index.

    Default to the file named in ``conf.doc_files`` for the given ``doc_type``.

    Returns:
        A set containing the IDs of the documents that were inserted.
    """
    if not input_path:
        input_path = conf.doc_files[doc_type]

    index_name = conf.graph_indices[doc_type]

    docs = []
    for doc in TestDataStats.load_es_graph_dump(input_path):
        to_append = {
            '_id': doc['{}_id'.format(doc_type)], '_index': index_name, '_source': doc
        }
        docs.append(to_append)

    # Remove .cases[] from underneath case.files[]
    if doc_type == 'case':
        for doc in docs:
            for _file in doc['_source']['files']:
                _file.pop('cases', None)

    # TODO: temp fix
    docs = remove_keys_from_dict(docs, ['file_state'])

    log.info('Bulk loading {} docs to the ES...'.format(doc_type))
    bulk(es, docs, ignore=409)

    log.info('loaded {} {} docs'.format(len(docs), doc_type))

    es.indices.refresh(index_name)

    ids = {doc['_id'] for doc in docs}
    return ids


@pytest.fixture(scope='session')
def es_client(setup_graph_indices):
    # The test config already sets up an ES client that we can just reuse.
    # TODO Probably refactor the way we use the test config so the test modules
    # don't create new ES clients upon import.
    return conf.es


@pytest.fixture(scope='session')
def source_es_client(setup_graph_indices):
    # TODO Again, reorganizing these ES clients would be cooool.
    return conf.source_es


@pytest.fixture
def index_cases_with_duplicate_aliquots(source_es_client, request):
    """Add cases with duplicate aliquot submitter IDs to the index.

    Remove them after the test completes.
    """
    input_path = os.path.join(conf.input_dir, 'cases_with_duplicate_aliquots.ndjson')
    ids = load_docs_into_test_index(source_es_client, 'case', input_path=input_path)

    yield ids

    body = {"query": {"terms": {'file_id': list(ids)}}}
    source_es_client.delete_by_query(index=conf.graph_case_index, body=body)


def files_with_linked_cases(source_es_client, request):
    input_path = os.path.join(conf.input_dir, 'files_with_linked_cases.ndjson')
    ids = load_docs_into_test_index(source_es_client, 'file', input_path=input_path)

    yield ids

    body = {"query": {"terms": {'file_id': list(ids)}}}
    source_es_client.delete_by_query(index=conf.graph_file_index, body=body)


@pytest.fixture(scope='session')
def sqlContext(es_client, source_es_client):
    sc = SparkContext('local[1]', 'sqlContextFixture')
    sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
    sqlCont = SQLContext(sc)
    sqlCont.sql("set spark.sql.shuffle.partitions=200")
    sqlCont.sql("set spark.sql.caseSensitive=true")
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
    cases = get_case_ids_from_source_es(conf, sqlContext)
    return {c.case_id for c in cases.collect()}


@pytest.fixture(scope='session')
def all_cases(source_es_client):
    """
    Returns the IDs of all cases in the GDC graph, including those with no
    maf or cnv data
    """
    hits = iterate_es_results(
        es_client=source_es_client,
        index_name=conf.graph_case_index,
        query={'_source': ['case_id']},
    )

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
    sub_case_df = case_df.drop('summary')
    builder = GeneCentricBuilder(conf, sqlContext)
    builder.build(maf_df, gistic_df, sub_case_df)

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
    sub_case_df = case_df.drop('summary')
    builder = SSMCentricBuilder(conf, sqlContext)
    builder.build(maf_df, sub_case_df)

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
    sub_case_df = case_df.drop('summary')
    builder = SSMOccurrenceCentricBuilder(conf, sqlContext)
    builder.build(maf_df, sub_case_df)

    log.info('\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    return builder.ssm_occurrence_centric


@pytest.fixture(scope='session')
def cnv_centric_df(sqlContext, gistic_df, case_df):
    """
    Builds cnv centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_CENTRIC DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = CNVCentricBuilder(conf, sqlContext)
    builder.build(gistic_df, sub_case_df)

    log.info('\n\n\tLOADING CNV_CENTRIC_DF\n\n')
    builder.load()
    return builder.cnv_centric


@pytest.fixture(scope='session')
def cnv_occurrence_centric_df(sqlContext, gistic_df, case_df):
    """
    Builds cnv occurrence centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_OCCURRENCE_CENTRIC DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = CNVOccurrenceCentricBuilder(conf, sqlContext)
    builder.build(gistic_df, sub_case_df)

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


@pytest.fixture(scope='module')
def raw_variant_caller_counts():
    """Get the expected number of observations for each caller in the raw MAFs.

    Hardcode based on the test data to minimize the risk of logic bugs in this
    fixture. Ensemble calls are not exploded when building the MAF DF, so list
    any ensemble calls verbatim.
    """
    return {
        'muse': 7,
        'mutect2': 11,
        'mutect2;muse*;somaticsniper': 1,
        'pindel': 3,
        'somaticsniper': 5,
        'varscan': 3,
    }


@pytest.fixture(scope='module')
def exploded_variant_caller_counts():
    """Get the expected number of observations for each caller after processing.

    Assume any ensemble calls have been split into individual observations.
    To update, ``grep -c`` for the various callers in the test MAFs.
    """
    return {
        'muse': 8,
        'mutect2': 12,
        'pindel': 3,
        'somaticsniper': 6,
        'varscan': 3,
    }
