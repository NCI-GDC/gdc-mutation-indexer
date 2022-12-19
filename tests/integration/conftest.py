import functools
import logging
import pathlib
import tempfile
import uuid
from typing import (
    AbstractSet,
    Any,
    Callable,
    Generator,
    Iterable,
    Iterator,
    List,
    Mapping,
    Union,
    cast,
)
from unittest import mock

import elasticsearch
import importlib_resources as resources
import pytest
import yaml
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import Literal

import config
from exports import builders, configuration, es_utils, indexd_utils, schemas
from exports.builders.clinical_annotations import civic
from exports.constants import build
from tests.integration.utils import test_setup

CentricIndexFinalizer = Callable[[build.IndexType], Callable[[], None]]
DataFrameWriter = Callable[[sql.DataFrame], sql.DataFrame]

log = logging.getLogger()
log.setLevel(logging.INFO)


@pytest.fixture(scope="session")
def dataframes_dir() -> Iterator[pathlib.Path]:
    with tempfile.TemporaryDirectory() as df_dir:
        yield pathlib.Path(df_dir)


@pytest.fixture(scope="session")
def data_dir() -> Iterator[pathlib.Path]:
    with resources.as_file(resources.files("tests.integration")) as module:
        yield module.joinpath("data")


@pytest.fixture(scope="session")
def input_dir(data_dir: pathlib.Path) -> pathlib.Path:
    return data_dir.joinpath("input")


@pytest.fixture(scope="session")
def maf_urls(input_dir: pathlib.Path) -> List[str]:
    return [str(p) for p in input_dir.joinpath("maf").glob("**/*.maf")]


@pytest.fixture(scope="session")
def configure_gene_model(input_dir: pathlib.Path) -> Callable[[dict], dict]:
    citobands_file = str(input_dir.joinpath("genes.cytobands.tsv.gz"))
    census_file = str(input_dir.joinpath("cancer_gene_census_set.tsv.gz"))
    gene_model_file = str(input_dir.joinpath("genes.ndjson.gz"))

    def pre_load(
        data: dict,
        drivers: Iterable[Literal["viz", "gene_expression"]] = (
            "viz",
            "gene_expression",
        ),
    ) -> dict:

        for driver in drivers:
            data["builders"][driver]["gene_model"]["citobands_file"] = citobands_file
            data["builders"][driver]["gene_model"]["census_file"] = census_file
            data["builders"][driver]["gene_model"]["gene_model_file"] = gene_model_file

        return data

    return pre_load


@pytest.fixture(scope="session")
def default_config(
    configure_gene_model: Callable[[dict], dict]
) -> configuration.Configuration:
    return test_setup.load_configuraiton(configure_gene_model)


@pytest.fixture(scope="session")
def es_client(
    default_config: configuration.Configuration,
) -> Iterator[elasticsearch.Elasticsearch]:
    # The test config already sets up an ES client that we can just reuse.
    # TODO Probably refactor the way we use the test config so the test modules
    # don't create new ES clients upon import.
    es_connection = default_config.elasticsearch.connection

    with elasticsearch.Elasticsearch(
        es_connection.nodes.split(","),
        use_ssl=es_connection.use_ssl,
        verify_certs=es_connection.verify_certs,
        http_auth=(
            es_connection.user,
            es_connection.password,
        ),
    ) as es_client:
        yield es_client


@pytest.fixture(scope="session")
def source_es_client(
    es_client: elasticsearch.Elasticsearch,
) -> elasticsearch.Elasticsearch:
    # TODO Again, reorganizing these ES clients would be cooool.
    return es_client


@pytest.fixture(scope="session")
def default_old_config(
    default_config: configuration.Configuration, es_client: elasticsearch.Elasticsearch
) -> config.BaseConfig:
    return config.ConfigAdapter(default_config, es_client, mock.MagicMock())


@pytest.fixture(scope="session")
def setup_graph_indices(
    default_config: configuration.Configuration,
    es_client: elasticsearch.Elasticsearch,
    input_dir: pathlib.Path,
) -> Iterator[Any]:
    """Create graph indices with required docs."""
    manager = test_setup.IndexManager(default_config, es_client, log)
    loader = test_setup.DocumentLoader(default_config, es_client, log)
    data = {
        build.IndexType.CASE: input_dir.joinpath("cases.ndjson.gz"),
        build.IndexType.FILE: input_dir.joinpath("files.ndjson.gz"),
    }

    with manager, loader:
        for index_type, input_file in data.items():
            loader.load_docs(index_type, input_file)

        yield True


@pytest.fixture(scope="class")
def files_with_linked_cases(
    setup_graph_indices: Any,  # Required to insure file index has been initialized.
    default_config: configuration.Configuration,
    es_client: elasticsearch.Elasticsearch,
    input_dir: pathlib.Path,
) -> Iterator[Any]:
    input_path = input_dir.joinpath("files_with_linked_cases.ndjson")

    with test_setup.DocumentLoader(default_config, es_client, log) as loader:
        yield loader.load_docs(build.IndexType.FILE, input_path)


@pytest.fixture(scope="session")
def spark_session() -> Generator[sql.SparkSession, None, None]:
    with sql.SparkSession.builder.master("local[*]").appName(
        "sqlContextFixture"
    ).config("spark.sql.shuffle.partitions", 1).config(
        "spark.ui.showConsoleProgress", False
    ).config(
        "spark.ui.enabled", False
    ).config(
        "spark.driver.memory", "2g"
    ).getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")
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
def all_maf_cases() -> AbstractSet[str]:
    """
    Returns all case_ids expected to build and have 'ssm' in available_variation_data
    (including "empty cases" - ones that have been tested for ssm but had none)
    The info is taken from aliquots in test maf headers
    """
    # Read aliquots from maf headers and get list of corresponding cases:
    return frozenset(
        (
            "13afbde8-e5b5-4f3c-8a9d-daef71560005",
            "d2748e35-4719-43c1-a533-b6b0cd9688c3",
            "ee8c1919-17a9-4df1-8aa5-79546621b23c",
            "d241a660-1c84-44fa-a6b3-ec9284333bd2",
            "2f5d8110-35c7-419f-8b35-bc3040f940f3",
            "1db41963-a520-47f0-828c-ed5c626507b1",
            "b08dfba8-6afb-4217-9259-72be6f1f3363",
            "452135f2-6de6-4593-a091-ddf6344ee431",
            "68642658-7996-4423-bb25-d3beb9a414f1",
            "f18cfe4a-fffd-4e09-9eef-343ba9ffd0d1",
            "a20aeafc-9a68-4af0-87ea-532ee835ebb2",
            "bbbce1ba-c739-43ba-b9cf-a4f746491ae3",
            "a29a20e3-5c2c-4f37-b93e-ae9ebc46ec53",
            "c689ae1d-4a6b-45db-b4d1-6b34c5c61522",
            "e8c2a8c6-5c2b-460b-b536-60bc537e6be3",
            "872092b3-d31e-44d7-bd03-e29f52f8ab5a",
        )
    )


@pytest.fixture(scope="session")
def all_cases(
    default_config: configuration.Configuration, es_client: elasticsearch.Elasticsearch
) -> AbstractSet[str]:
    """
    Returns the IDs of all cases in the GDC graph, including those with no
    maf or cnv data
    """
    hits = es_utils.iterate_es_results(
        es_client=es_client,
        index_name=default_config.elasticsearch.read.case_index,
        query={"_source": ["case_id"]},
    )

    return {hit["_source"]["case_id"] for hit in hits}


@pytest.fixture(scope="session")
def dataframe_writer(
    spark_session: sql.SparkSession, dataframes_dir: pathlib.Path
) -> DataFrameWriter:
    def write(df: sql.DataFrame) -> sql.DataFrame:
        path = str(dataframes_dir.joinpath(f"{uuid.uuid4()}.parquet"))

        df.write.parquet(path)

        return spark_session.read.parquet(path)

    return write


@pytest.fixture(scope="session")
def gene_model_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    df = builders.GeneModelBuilder(
        default_config.builders.viz.gene_model, spark_session
    ).build()

    return dataframe_writer(df)


@pytest.fixture(scope="session")
def maf_df(
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_urls: List[str],
    gene_model_df: sql.DataFrame,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds combined maf dataframe once. Reused throughout test suite
    """
    log.info("\n\n\tBUILDING MAF_DF\n\n")
    maf_df = (
        sqlContext.read.csv(
            maf_urls,
            sep="\t",
            header=True,
            comment="#",
        )
        .drop(
            "AFR_MAF",
            "ALLELE_NUM",
            "AMR_MAF",
            "ASN_MAF",
            "EAS_MAF",
            "EA_MAF",
            "EUR_MAF",
            "ExAC_AF",
            "ExAC_AF_AFR",
            "ExAC_AF_AMR",
            "ExAC_AF_Adj",
            "ExAC_AF_EAS",
            "ExAC_AF_FIN",
            "ExAC_AF_NFE",
            "ExAC_AF_OTH",
            "ExAC_AF_SAS",
            "FILTER",
            "GDC_Validation_Status",
            "GMAF",
            "MC3_Overlap",
            "MINIMISED",
            "SAS_MAF",
        )
        .select(
            "*",
            *(
                F.lit(None).cast(types.StringType()).alias(name)
                for name in (
                    "1000G_AF",
                    "1000G_AFR_AF",
                    "1000G_AMR_AF",
                    "1000G_EAS_AF",
                    "1000G_EUR_AF",
                    "1000G_SAS_AF",
                    "APPRIS",
                    "ESP_EA_AF",
                    "FLAGS",
                    "MANE",
                    "MAX_AF",
                    "MAX_AF_POPS",
                    "RNA_Support",
                    "RNA_alt_count",
                    "RNA_depth",
                    "RNA_ref_count",
                    "TRANSCRIPTION_FACTORS",
                    "UNIPROT_ISOFORM",
                    "gnomAD_AF",
                    "gnomAD_AFR_AF",
                    "gnomAD_AMR_AF",
                    "gnomAD_ASJ_AF",
                    "gnomAD_EAS_AF",
                    "gnomAD_FIN_AF",
                    "gnomAD_NFE_AF",
                    "gnomAD_OTH_AF",
                    "gnomAD_SAS_AF",
                    "gnomAD_non_cancer_AF",
                    "gnomAD_non_cancer_AFR_AF",
                    "gnomAD_non_cancer_AMI_AF",
                    "gnomAD_non_cancer_AMR_AF",
                    "gnomAD_non_cancer_ASJ_AF",
                    "gnomAD_non_cancer_EAS_AF",
                    "gnomAD_non_cancer_FIN_AF",
                    "gnomAD_non_cancer_MAX_AF_POPS_adj",
                    "gnomAD_non_cancer_MAX_AF_adj",
                    "gnomAD_non_cancer_MID_AF",
                    "gnomAD_non_cancer_NFE_AF",
                    "gnomAD_non_cancer_OTH_AF",
                    "gnomAD_non_cancer_SAS_AF",
                    "hotspot",
                    "miRNA",
                )
            ),
        )
    )
    fm_ad_maf_df = sqlContext.createDataFrame(
        (), schema=schemas.load_schema("builders/maf/aggregated_somatic_mutation.yaml")
    )
    doc_dataframe_util = mock.MagicMock(spec=indexd_utils.DataFrameUtil)
    doc_dataframe_util.get_dataframe.side_effect = (maf_df, fm_ad_maf_df)

    df = builders.MAFBuilder(
        default_old_config,
        sqlContext,
        doc_dataframe_util,
        (civic.CivicBuilder(default_old_config, sqlContext),),
    ).build(gene_model_df=gene_model_df, maf_metadata_df=mock.MagicMock())

    return dataframe_writer(df)


@pytest.fixture(scope="session")
def cnv_df(spark_session: sql.SparkSession, data_dir: pathlib.Path) -> sql.DataFrame:
    """
    Builds combined cnv dataframe once. Reused throughout test suite
    """
    cnv_dir = data_dir.joinpath("input/cnv")

    with open(cnv_dir.joinpath("schema.yaml"), "r") as f:
        schema = types.StructType.fromJson(yaml.safe_load(f))

    return spark_session.read.json(
        str(cnv_dir.joinpath("data.ndjson.gz")), schema=schema
    )


@pytest.fixture(scope="session")
def maf_metadata_df(
    sqlContext: sql.SQLContext, all_maf_cases: Iterable[str]
) -> sql.DataFrame:
    return sqlContext.createDataFrame(
        tuple((case_id,) for case_id in all_maf_cases), schema="case_id: string"
    ).cache()


@pytest.fixture(scope="session")
def case_df(
    default_old_config: config.BaseConfig,
    default_config: configuration.Configuration,
    sqlContext: sql.SQLContext,
    spark_session: sql.SparkSession,
    maf_metadata_df: sql.DataFrame,
    maf_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
    es_client: elasticsearch.Elasticsearch,
    dataframe_writer: DataFrameWriter,
    setup_graph_indices: Any,
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(
        default_old_config, sqlContext, es_client
    )
    df = builders.CaseBuilder(
        default_config.builders.viz.case,
        spark_session,
        es_dataframe_util,
        es_utils.CaseFieldSelector(),
    ).build(maf_metadata_df=maf_metadata_df, maf_df=maf_df, ascat_df=cnv_df)

    return dataframe_writer(df)


@pytest.fixture(scope="session")
def ssm_transcript_df(
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
) -> sql.DataFrame:
    """
    Builds ssm-transcript dataframe once. Reused throughout test suite
    This is a maf_df with flattend and filtered according to all_effects.do_not_use transcripts
    """
    log.info("\n\n\tBUILDING SSM_TRANSCRIPT_DF\n\n")
    return builders.ConsequenceBuilder(
        default_old_config, sqlContext
    ).build_all_effects_cols(maf_df)


@pytest.fixture(scope="session")
def primary_aliquot_df(sqlContext: sql.SQLContext) -> sql.DataFrame:
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
def consequence_builder(
    default_old_config: config.BaseConfig, sqlContext: sql.SQLContext
) -> builders.ConsequenceBuilder:
    return builders.ConsequenceBuilder(default_old_config, sqlContext)


@pytest.fixture(scope="session")
def observation_builder() -> builders.ObservationBuilder:
    return builders.ObservationBuilder()


@pytest.fixture(scope="session")
def centric_index_finalizer(
    default_config: configuration.Configuration, es_client: elasticsearch.Elasticsearch
) -> CentricIndexFinalizer:
    def finalizer(index_type: build.IndexType) -> None:
        indices = default_config.elasticsearch.write.indices

        es_client.indices.delete(index=indices[index_type], ignore=(404,))

    return lambda it: functools.partial(finalizer, it)


@pytest.fixture(scope="session")
def case_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_metadata_df: sql.DataFrame,
    maf_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    es_client: elasticsearch.Elasticsearch,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
    setup_graph_indices: Any,
) -> sql.DataFrame:
    """
    Builds case centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    request.addfinalizer(centric_index_finalizer(build.IndexType.CASE_CENTRIC))
    log.info("\n\n\tBUILDING CASE_CENTRIC_DF\n\n")
    builder = builders.CaseCentricBuilder(
        default_old_config,
        sqlContext,
        es_utils.DataFrameUtil(default_old_config, sqlContext, es_client),
        es_utils.RDDUtil(default_old_config, sqlContext.sparkSession.sparkContext),
        es_utils.CaseFieldSelector(),
        consequence_builder,
        observation_builder,
    )

    builder.build(maf_metadata_df, maf_df, cnv_df, primary_aliquot_df)

    log.info("\n\n\tLOADING CASE_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.case_centric))


@pytest.fixture(scope="session")
def gene_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds gene centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    request.addfinalizer(centric_index_finalizer(build.IndexType.GENE_CENTRIC))
    log.info("\n\n\tBUILDING GENE_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.GeneCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, cnv_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING GENE_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.gene_centric))


@pytest.fixture(scope="session")
def ssm_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    case_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds ssm centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    request.addfinalizer(centric_index_finalizer(build.IndexType.SSM_CENTRIC))
    log.info("\n\n\tBUILDING SSM_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.SSMCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING SSM_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.ssm_centric))


@pytest.fixture(scope="session")
def ssm_occurrence_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    case_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds ssm occurrence centric dataframe once. Loads to elasticsearch index
    Reused throughout test suite
    """
    request.addfinalizer(
        centric_index_finalizer(build.IndexType.SSM_OCCURRENCE_CENTRIC)
    )
    log.info("\n\n\tBUILDING SSM_OCCURRENCE_CENTRIC_DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.SSMOccurrenceCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    builder.build(maf_df, sub_case_df, primary_aliquot_df)

    log.info("\n\n\tLOADING SSM_OCCURRENCE_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.ssm_occurrence_centric))


@pytest.fixture(scope="session")
def cnv_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds cnv centric dataframe
    """
    request.addfinalizer(centric_index_finalizer(build.IndexType.CNV_CENTRIC))
    log.info("\n\n\tBUILDING CNV_CENTRIC DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.CNVCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    builder.build(cnv_df, sub_case_df)

    log.info("\n\n\tLOADING CNV_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.cnv_centric))


@pytest.fixture(scope="session")
def cnv_occurrence_centric_df(
    request: pytest.FixtureRequest,
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
    centric_index_finalizer: CentricIndexFinalizer,
    dataframe_writer: DataFrameWriter,
) -> sql.DataFrame:
    """
    Builds cnv occurrence centric dataframe
    """
    request.addfinalizer(
        centric_index_finalizer(build.IndexType.CNV_OCCURRENCE_CENTRIC)
    )
    log.info("\n\n\tBUILDING CNV_OCCURRENCE_CENTRIC DF\n\n")
    sub_case_df = case_df.drop("summary")
    builder = builders.CNVOccurrenceCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    builder.build(cnv_df, sub_case_df)

    log.info("\n\n\tLOADING CNV_OCCURRENCE_CENTRIC_DF\n\n")
    builder.load()

    return dataframe_writer(cast(sql.DataFrame, builder.cnv_occurrence_centric))


@pytest.fixture(scope="session")
def case_ssm_subtree(
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    es_client: elasticsearch.Elasticsearch,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
) -> sql.DataFrame:
    """
    Builds case centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING CASE_SSM_SUBTREE\n\n")
    builder = builders.CaseCentricBuilder(
        default_old_config,
        sqlContext,
        es_utils.DataFrameUtil(default_old_config, sqlContext, es_client),
        es_utils.RDDUtil(default_old_config, sqlContext.sparkSession.sparkContext),
        es_utils.CaseFieldSelector(),
        consequence_builder,
        observation_builder,
    )

    return builder.build_ssm_subtree(maf_df, primary_aliquot_df)


@pytest.fixture(scope="session")
def gene_ssm_subtree(
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
) -> sql.DataFrame:
    """
    Builds gene centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING GENE_SSM_SUBTREE\n\n")
    builder = builders.GeneCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    return builder.build_ssm_subtree(maf_df, primary_aliquot_df)


@pytest.fixture(scope="session")
def ssm_occurrence_ssm_subtree(
    default_old_config: config.BaseConfig,
    sqlContext: sql.SQLContext,
    maf_df: sql.DataFrame,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
):
    """
    Builds ssm occurrence centric ssm subtree dataframe
    """
    log.info("\n\n\tBUILDING SSM_OCCURRENCE_SSM_SUBTREE\n\n")
    builder = builders.SSMOccurrenceCentricBuilder(
        default_old_config, sqlContext, consequence_builder, observation_builder
    )

    return builder.build_ssm_subtree(maf_df)


@pytest.fixture(scope="module")
def raw_variant_caller_counts() -> Mapping[str, int]:
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
        "somaticsniper;muse": 5,
        "varscan": 3,
    }


@pytest.fixture(scope="module")
def exploded_variant_caller_counts() -> Mapping[str, int]:
    """Get the expected number of observations for each caller after processing.

    Assume any ensemble calls have been split into individual observations.
    To update, ``grep -c`` for the various callers in the test MAFs.
    """
    return {
        "muse": 13,
        "mutect2": 12,
        "pindel": 3,
        "varscan": 3,
    }


@pytest.fixture(scope="function")
def load_data_from_file(
    data_dir: pathlib.Path,
) -> Callable[[Union[str, pathlib.Path]], Any]:
    def load(filename: Union[str, pathlib.Path]):
        with open(data_dir.joinpath(filename)) as f:
            return yaml.safe_load(f)

    return load
