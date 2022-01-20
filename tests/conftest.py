import logging
import os
from typing import Generator

import elasticsearch
import pytest
import yaml
from pyspark import sql
from pyspark.sql import types
from exports.builders.clinical_annotations import civic

import tests_config
from exports import builders, es_utils
from exports.builders import utils
from tests.utils import maf_metrics, test_setup, true_stats

conf = tests_config.TestConfig()

log = logging.getLogger()
log.setLevel(logging.INFO)


GRAPH_INDICES = frozenset(["case", "file"])


@pytest.fixture(scope="session")
def setup_graph_indices() -> Generator[bool, None, None]:
    """Create graph indices with required docs."""
    manager = test_setup.IndexManager(conf, conf.es, log, GRAPH_INDICES)
    loader = test_setup.DocumentLoader(conf, conf.es, log)

    with manager, loader:
        manager.create_indices()

        for doc_type in GRAPH_INDICES:
            loader.load_docs(doc_type)

        yield True


@pytest.fixture(scope="session")
def es_client(setup_graph_indices):
    # The test config already sets up an ES client that we can just reuse.
    # TODO Probably refactor the way we use the test config so the test modules
    # don't create new ES clients upon import.
    return conf.es


@pytest.fixture(scope="session")
def source_es_client(setup_graph_indices):
    # TODO Again, reorganizing these ES clients would be cooool.
    return conf.source_es


@pytest.fixture
def index_cases_with_duplicate_aliquots(source_es_client):
    """Add cases with duplicate aliquot submitter IDs to the index.

    Remove them after the test completes.
    """
    input_path = os.path.join(conf.input_dir, "cases_with_duplicate_aliquots.ndjson")

    with test_setup.DocumentLoader(conf, source_es_client, log) as loader:
        yield loader.load_docs("case", input_path=input_path)


@pytest.fixture(scope="class")
def files_with_linked_cases(source_es_client):
    input_path = os.path.join(conf.input_dir, "files_with_linked_cases.ndjson")

    with test_setup.DocumentLoader(conf, source_es_client, log) as loader:
        yield loader.load_docs("file", input_path=input_path)


@pytest.fixture(scope="session")
def spark_session() -> Generator[sql.SparkSession, None, None]:
    with sql.SparkSession.builder.master("local[1]").appName(
        "sqlContextFixture"
    ).getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")
        spark_session.sql("set spark.sql.shuffle.partitions=200")
        spark_session.sql("set spark.sql.caseSensitive=true")

        yield spark_session


@pytest.fixture(scope="session")
def sqlContext(
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    source_es_client: elasticsearch.Elasticsearch,
) -> sql.SQLContext:
    return sql.SQLContext(spark_session.sparkContext)


@pytest.fixture(scope="session")
def all_maf_cases(sqlContext, maf_df):
    """
    Returns all case_ids expected to build and have 'ssm' in available_variation_data
    (including "empty cases" - ones that have been tested for ssm but had none)
    The info is taken from aliquots in test maf headers
    """
    # Read aliquots from maf headers and get list of corresponding cases:
    cases = utils.get_case_ids_from_source_es(conf, sqlContext)
    return {c.case_id for c in cases.collect()}


@pytest.fixture(scope="session")
def all_cases(source_es_client):
    """
    Returns the IDs of all cases in the GDC graph, including those with no
    maf or cnv data
    """
    hits = es_utils.iterate_es_results(
        es_client=source_es_client,
        index_name=conf.graph_case_index,
        query={"_source": ["case_id"]},
    )

    return {hit["_source"]["case_id"] for hit in hits}


@pytest.fixture(scope="session")
def test_data():
    return true_stats.TestDataStats.load_test_data(conf.input_dir)


@pytest.fixture(scope="session")
def gene_model_df(sqlContext) -> sql.DataFrame:
    return builders.GeneModelBuilder(conf, sqlContext).build()


@pytest.fixture(scope="session")
def maf_df(sqlContext, gene_model_df):
    """
    Builds combined maf dataframe once. Reused throughout test suite
    """
    log.info("\n\n\tBUILDING MAF_DF\n\n")
    return builders.MAFBuilder(conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)).build(gene_model_df=gene_model_df)


@pytest.fixture(scope="session")
def gistic_df(sqlContext, gene_model_df):
    """
    Builds combined gistic dataframe once. Reused throughout test suite
    """
    log.info("\n\n\tBUILDING GISTIC_DF\n\n")
    return builders.GisticBuilder(conf, sqlContext).build(gene_model_df=gene_model_df)


@pytest.fixture(scope="session")
def case_df(sqlContext, maf_df, gistic_df):
    return builders.CaseBuilder(conf, sqlContext).build(maf_df, gistic_df)


@pytest.fixture(scope="session")
def ssm_transcript_df(sqlContext, maf_df):
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    log.info("\n\n\tBUILDING SSM_TRANSCRIPT_DF\n\n")
    return builders.ConsequenceBuilder(conf, sqlContext).build_all_effects_cols(maf_df)


@pytest.fixture(scope="session")
def primary_aliquot_df(sqlContext):
    """
    Builds a dataframe of primary aliquot selections for each of the cases in
    the test data.
    """
    primary_aliquots = [
        ("case", "1db41963-a520-47f0-828c-ed5c626507b1", "WXG"),
        ("case", "0ff579a1-e295-408d-b194-febbca798e34", "WSG"),
        ("case", "872092b3-d31e-44d7-bd03-e29f52f8ab5a", "WSG"),
        ("case", "bbbce1ba-c739-43ba-b9cf-a4f746491ae3", "WXG"),
        ("case", "2f5d8110-35c7-419f-8b35-bc3040f940f3", "WXG"),
        ("case", "13afbde8-e5b5-4f3c-8a9d-daef71560005", "WXG"),
        ("case", "e8c2a8c6-5c2b-460b-b536-60bc537e6be3", "WXG"),
        ("case", "b08dfba8-6afb-4217-9259-72be6f1f3363", "WXG"),
        ("case", "68642658-7996-4423-bb25-d3beb9a414f1", "WXG"),
        ("case", "a29a20e3-5c2c-4f37-b93e-ae9ebc46ec53", "WXG"),
        ("case", "f18cfe4a-fffd-4e09-9eef-343ba9ffd0d1", "WXG"),
        ("case", "c689ae1d-4a6b-45db-b4d1-6b34c5c61522", "WSG"),
        ("case", "d2748e35-4719-43c1-a533-b6b0cd9688c3", "WXG"),
        ("case", "d241a660-1c84-44fa-a6b3-ec9284333bd2", "WSG"),
        ("case", "00000000-1111-2222-4444-888888888888", "WSG"),
        ("case", "ee8c1919-17a9-4df1-8aa5-79546621b23c", "WSG"),
        ("case", "452135f2-6de6-4593-a091-ddf6344ee431", "WXG"),
        ("case", "a20aeafc-9a68-4af0-87ea-532ee835ebb2", "WSG"),
    ]
    schema = types.StructType(
        [
            types.StructField("entity", types.StringType()),
            types.StructField("case_id", types.StringType()),
            types.StructField("experimental_strategy", types.StringType()),
        ]
    )

    return sqlContext.createDataFrame(primary_aliquots, schema)


@pytest.fixture(scope="session")
def consequence_builder(sqlContext):
    return builders.ConsequenceBuilder(conf, sqlContext)


@pytest.fixture(scope="session")
def observation_builder():
    return builders.ObservationBuilder()


@pytest.fixture(scope="session")
def case_centric_df(
    sqlContext,
    maf_df,
    gistic_df,
    case_df,
    primary_aliquot_df,
    consequence_builder,
    observation_builder,
):
    """
    Builds case centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info("\n\n\tBUILDING CASE_CENTRIC_DF\n\n")
    builder = builders.CaseCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, gistic_df, case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING CASE_CENTRIC_DF\n\n")
    builder.load()

    return builder.case_centric


@pytest.fixture(scope="session")
def gene_centric_df(
    sqlContext,
    maf_df,
    gistic_df,
    case_df,
    primary_aliquot_df,
    consequence_builder,
    observation_builder,
):
    """
    Builds gene centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info("\n\n\tBUILDING GENE_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.GeneCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, gistic_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING GENE_CENTRIC_DF\n\n")
    builder.load()

    return builder.gene_centric


@pytest.fixture(scope="session")
def ssm_centric_df(
    sqlContext,
    maf_df,
    case_df,
    primary_aliquot_df,
    consequence_builder,
    observation_builder,
):
    """
    Builds ssm centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info("\n\n\tBUILDING SSM_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.SSMCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING SSM_CENTRIC_DF\n\n")
    builder.load()

    return builder.ssm_centric


@pytest.fixture(scope="session")
def ssm_occurrence_centric_df(
    sqlContext,
    maf_df,
    case_df,
    primary_aliquot_df,
    consequence_builder,
    observation_builder,
):
    """
    Builds ssm occurrence centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    log.info("\n\n\tBUILDING SSM_OCCURRENCE_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.SSMOccurrenceCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n")
    builder.load()

    return builder.ssm_occurrence_centric


@pytest.fixture(scope="session")
def cnv_centric_df(
    sqlContext, gistic_df, case_df, consequence_builder, observation_builder
):
    """
    Builds cnv centric dataframe
    """
    log.info("\n\n\tBUILDING CNV_CENTRIC DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.CNVCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(gistic_df, sub_case_df)

    log.info("\n\n\tLOADING CNV_CENTRIC_DF\n\n")
    builder.load()

    return builder.cnv_centric


@pytest.fixture(scope="session")
def cnv_occurrence_centric_df(
    sqlContext, gistic_df, case_df, consequence_builder, observation_builder
):
    """
    Builds cnv occurrence centric dataframe
    """
    log.info("\n\n\tBUILDING CNV_OCCURRENCE_CENTRIC DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.CNVOccurrenceCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    builder.build(gistic_df, sub_case_df)

    log.info("\n\n\tLOADING CNV_OCCURRENCE_CENTRIC_DF\n\n")
    builder.load()

    return builder.cnv_occurrence_centric


@pytest.fixture(scope="session")
def case_ssm_subtree(
    sqlContext, maf_df, primary_aliquot_df, consequence_builder, observation_builder
):
    """
    Builds case centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING CASE_SSM_SUBTREE\n\n")
    builder = builders.CaseCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    return builder.build_ssm_subtree(maf_df, primary_aliquot_df)


@pytest.fixture(scope="session")
def gene_ssm_subtree(
    sqlContext, maf_df, primary_aliquot_df, consequence_builder, observation_builder
):
    """
    Builds gene centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING GENE_SSM_SUBTREE\n\n")
    builder = builders.GeneCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    return builder.build_ssm_subtree(maf_df, primary_aliquot_df)


@pytest.fixture(scope="session")
def ssm_occurrence_ssm_subtree(
    sqlContext, maf_df, consequence_builder, observation_builder
):
    """
    Builds ssm occurrence centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING SSM_OCCURRENCE_SSM_SUBTREE\n\n")
    builder = builders.SSMOccurrenceCentricBuilder(
        conf, sqlContext, consequence_builder, observation_builder
    )

    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope="module")
def maf_stats():
    yield maf_metrics.MAFStats(conf.maf_urls)


@pytest.fixture(scope="module")
def raw_variant_caller_counts():
    """Get the expected number of observations for each caller in the raw MAFs.

    Hardcode based on the test data to minimize the risk of logic bugs in this
    fixture. Ensemble calls are not exploded when building the MAF DF, so list
    any ensemble calls verbatim.
    """
    return {
        "muse": 7,
        "mutect2": 11,
        "mutect2;muse*;somaticsniper": 1,
        "pindel": 3,
        "somaticsniper": 5,
        "varscan": 3,
    }


@pytest.fixture(scope="module")
def exploded_variant_caller_counts():
    """Get the expected number of observations for each caller after processing.

    Assume any ensemble calls have been split into individual observations.
    To update, ``grep -c`` for the various callers in the test MAFs.
    """
    return {
        "muse": 8,
        "mutect2": 12,
        "pindel": 3,
        "somaticsniper": 6,
        "varscan": 3,
    }


@pytest.fixture(scope="function")
def load_data_from_file():
    def load(filename):
        with open(os.path.join(conf.data_dir, filename)) as f:
            return yaml.safe_load(f)

    return load
