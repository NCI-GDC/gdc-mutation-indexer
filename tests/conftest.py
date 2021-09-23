import logging
import os
from unittest.mock import MagicMock

import pytest
import yaml

from cdisutils.dictionary import remove_keys_from_dict
from elasticsearch.helpers import bulk
from exports.builders import (
    CaseBuilder,
    CaseCentricBuilder,
    CNVCentricBuilder,
    CNVOccurrenceCentricBuilder,
    ConsequenceBuilder,
    GeneCentricBuilder,
    GisticBuilder,
    MAFBuilder,
    ObservationBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)
from exports.builders.utils import get_case_ids_from_source_es
from exports.es_utils import iterate_es_results
from normalizer.mapper import ModelMapper
from pyspark import sql
from pyspark.sql.types import StringType, StructField, StructType
from tests.utils.maf_metrics import MAFStats
from tests.utils.true_stats import TestDataStats
from tests_config import TestConfig

conf = TestConfig()

log = logging.getLogger()
log.setLevel(logging.INFO)


GRAPH_INDICES = frozenset(['case', 'file'])


@pytest.fixture(scope='session')
def setup_graph_indices():
    """Create graph indices with required docs."""
    es = conf.es
    for doc_type in GRAPH_INDICES:
        print('\n\n\tSETTING UP {} TEST INDICES\n\n'.format(doc_type.upper()))
        if create_test_index(es, doc_type=doc_type):
            load_docs_into_test_index(es, doc_type)


def create_test_index(es, doc_type):
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

    log.info('Bulk loading {} docs to the ES... {}'.format(doc_type, len(docs)))
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


@pytest.fixture(scope="class")
def files_with_linked_cases(source_es_client, request):
    input_path = os.path.join(conf.input_dir, 'files_with_linked_cases.ndjson')
    ids = load_docs_into_test_index(source_es_client, 'file', input_path=input_path)

    yield ids

    body = {"query": {"terms": {'file_id': list(ids)}}}
    source_es_client.delete_by_query(index=conf.graph_file_index, body=body)


@pytest.fixture(scope='session')
def spark_session(es_client, source_es_client):
    builder = sql.SparkSession.builder.master('local[1]').appName('sqlContextFixture').config("spark.ui.showConsoleProgress", False).config("log4j.rootCategory", "FATAL")
    
    with builder.getOrCreate() as spark_session:
        spark_session.sql("set spark.sql.shuffle.partitions=200")
        spark_session.sql("set spark.sql.caseSensitive=true")

        yield spark_session

    spark_session._jvm.System.clearProperty("spark.driver.port")


@pytest.fixture(scope='session')
def all_maf_cases(spark_session, maf_df):
    """
    Returns all case_ids expected to build and have 'ssm' in available_variation_data
    (including "empty cases" - ones that have been tested for ssm but had none)
    The info is taken from aliquots in test maf headers
    """
    # Read aliquots from maf headers and get list of corresponding cases:
    cases = get_case_ids_from_source_es(conf, spark_session)
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
def maf_df(spark_session):
    """
    Builds combined maf dataframe once. Reused throughout test suite
    """
    log.info('\n\n\tBUILDING MAF_DF\n\n')
    return MAFBuilder(conf, spark_session).build()


@pytest.fixture(scope="session")
def gistic_df(spark_session):
    """
    Builds combined gistic dataframe once. Reused throughout test suite
    """
    log.info('\n\n\tBUILDING GISTIC_DF\n\n')
    return GisticBuilder(conf, spark_session).build()


@pytest.fixture(scope='session')
def case_df(spark_session, maf_df, gistic_df):
    return CaseBuilder(conf, spark_session).build(maf_df, gistic_df)


@pytest.fixture(scope='session')
def ssm_transcript_df(spark_session, maf_df):
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    log.info('\n\n\tBUILDING SSM_TRANSCRIPT_DF\n\n')
    return ConsequenceBuilder(conf, spark_session).build_all_effects_cols(maf_df)


@pytest.fixture(scope="session")
def primary_aliquot_builder(spark_session):
    """
    Builds a dataframe of primary aliquot selections for each of the cases in
    the test data.
    """
    primary_aliquot_builder = MagicMock()
    primary_aliquots = [
        ("1db41963-a520-47f0-828c-ed5c626507b1", "WXG"),
        ("0ff579a1-e295-408d-b194-febbca798e34", "WSG"),
        ("872092b3-d31e-44d7-bd03-e29f52f8ab5a", "WSG"),
        ("bbbce1ba-c739-43ba-b9cf-a4f746491ae3", "WXG"),
        ("2f5d8110-35c7-419f-8b35-bc3040f940f3", "WXG"),
        ("13afbde8-e5b5-4f3c-8a9d-daef71560005", "WXG"),
        ("e8c2a8c6-5c2b-460b-b536-60bc537e6be3", "WXG"),
        ("b08dfba8-6afb-4217-9259-72be6f1f3363", "WXG"),
        ("68642658-7996-4423-bb25-d3beb9a414f1", "WXG"),
        ("a29a20e3-5c2c-4f37-b93e-ae9ebc46ec53", "WXG"),
        ("f18cfe4a-fffd-4e09-9eef-343ba9ffd0d1", "WXG"),
        ("c689ae1d-4a6b-45db-b4d1-6b34c5c61522", "WSG"),
        ("d2748e35-4719-43c1-a533-b6b0cd9688c3", "WXG"),
        ("d241a660-1c84-44fa-a6b3-ec9284333bd2", "WSG"),
        ("00000000-1111-2222-4444-888888888888", "WSG"),
        ("ee8c1919-17a9-4df1-8aa5-79546621b23c", "WSG"),
        ("452135f2-6de6-4593-a091-ddf6344ee431", "WXG"),
        ("a20aeafc-9a68-4af0-87ea-532ee835ebb2", "WSG"),
    ]
    schema = StructType([
        StructField("case_id", StringType()),
        StructField("experimental_strategy", StringType())
    ])
    primary_aliquot_df = spark_session.createDataFrame(primary_aliquots, schema)

    primary_aliquot_builder.build_primary_aliquots_for_project.return_value = primary_aliquot_df

    return primary_aliquot_builder


@pytest.fixture(scope='session')
def consequence_builder(spark_session):
    return ConsequenceBuilder(conf, spark_session)


@pytest.fixture(scope='session')
def observation_builder(primary_aliquot_builder):
    return ObservationBuilder(primary_aliquot_builder)


@pytest.fixture(scope='session')
def case_centric_df(spark_session, maf_df, gistic_df, case_df, consequence_builder, observation_builder):
    """
    Builds case centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING CASE_CENTRIC_DF\n\n')
    builder = CaseCentricBuilder(conf, spark_session, consequence_builder, observation_builder)

    builder.build(maf_df, gistic_df, case_df)

    log.info('\n\n\tLOADING CASE_CENTRIC_DF\n\n')
    builder.load()
        
    return builder.case_centric


@pytest.fixture(scope='session')
def gene_centric_df(spark_session, maf_df, gistic_df, case_df, consequence_builder, observation_builder):
    """
    Builds gene centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING GENE_CENTRIC_DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = GeneCentricBuilder(conf, spark_session, consequence_builder, observation_builder)
    
    builder.build(maf_df, gistic_df, sub_case_df)

    log.info('\n\n\tLOADING GENE_CENTRIC_DF\n\n')
    builder.load()
    
    return builder.gene_centric


@pytest.fixture(scope='session')
def ssm_centric_df(spark_session, maf_df, case_df, consequence_builder, observation_builder):
    """
    Builds ssm centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_CENTRIC_DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = SSMCentricBuilder(conf, spark_session, consequence_builder, observation_builder)

    builder.build(maf_df, sub_case_df)

    log.info('\n\n\tLOADING SSM_CENTRIC_DF\n\n')
    builder.load()
    
    return builder.ssm_centric


@pytest.fixture(scope='session')
def ssm_occurrence_centric_df(spark_session, maf_df, case_df, consequence_builder, observation_builder):
    """
    Builds ssm occurrence centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info('\n\n\tBUILDING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = SSMOccurrenceCentricBuilder(conf, spark_session, consequence_builder, observation_builder)

    builder.build(maf_df, sub_case_df)

    log.info('\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    
    return builder.ssm_occurrence_centric


@pytest.fixture(scope='session')
def cnv_centric_df(spark_session, gistic_df, case_df, consequence_builder, observation_builder):
    """
    Builds cnv centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_CENTRIC DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = CNVCentricBuilder(conf, spark_session, consequence_builder, observation_builder)
    
    builder.build(gistic_df, sub_case_df)

    log.info('\n\n\tLOADING CNV_CENTRIC_DF\n\n')
    builder.load()
    
    return builder.cnv_centric


@pytest.fixture(scope='session')
def cnv_occurrence_centric_df(spark_session, gistic_df, case_df, consequence_builder, observation_builder):
    """
    Builds cnv occurrence centric dataframe
    """
    log.info('\n\n\tBUILDING CNV_OCCURRENCE_CENTRIC DF\n\n')
    sub_case_df = case_df.drop('summary')
    builder = CNVOccurrenceCentricBuilder(conf, spark_session, consequence_builder, observation_builder)

    builder.build(gistic_df, sub_case_df)

    log.info('\n\n\tLOADING CNV_OCCURRENCE_CENTRIC_DF\n\n')
    builder.load()
    
    return builder.cnv_occurrence_centric


@pytest.fixture(scope='session')
def case_ssm_subtree(spark_session, maf_df, consequence_builder, observation_builder):
    """
    Builds case centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING CASE_SSM_SUBTREE\n\n')
    builder = CaseCentricBuilder(conf, spark_session, consequence_builder, observation_builder)
        
    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope='session')
def gene_ssm_subtree(spark_session, maf_df, consequence_builder, observation_builder):
    """
    Builds gene centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING GENE_SSM_SUBTREE\n\n')
    builder = GeneCentricBuilder(conf, spark_session, consequence_builder, observation_builder)
        
    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope='session')
def ssm_occurrence_ssm_subtree(spark_session, maf_df, consequence_builder, observation_builder):
    """
    Builds ssm occurrence centric ssm subtree dataframe
    """
    log.info('\n\n\tBUILDING SSM_OCCURRENCE_SSM_SUBTREE\n\n')
    builder = SSMOccurrenceCentricBuilder(conf, spark_session, consequence_builder, observation_builder)
    
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


@pytest.fixture(scope="function")
def load_data_from_file():
    def load(filename):
        with open(os.path.join(conf.data_dir, filename)) as f:
            return yaml.safe_load(f)

    return load
