import contextlib
import logging
import types
from typing import Iterator, Mapping, Union

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

import config as old_config
from exports import builders, configuration, es_utils, gdc_mutation_export, indexd_utils
from exports.builders import (
    ascat,
    base_builder,
    base_input_builder,
    bases,
    maf_metadata,
)
from exports.builders.clinical_annotations import civic
from exports.configuration import elasticsearch as es_config
from exports.configuration import indexd
from exports.constants import app, build

logger = logging.getLogger("exports")

logging.basicConfig(format=app.LOG_FORMAT, level=logging.INFO)


@contextlib.contextmanager
def initialize_spark() -> Iterator[sql.SparkSession]:
    """
    Initializes the spark session.

    Returns:
        The context manager for spark session for the current driver.
    """
    with sql.SparkSession.builder.getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session


def get_index_client(config: indexd.IndexD) -> client.IndexClient:
    """
    Builds the index client with the given configuration values.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An indexd client
    """
    return client.IndexClient(
        baseurl=f"{config.host}:{config.port}",
        auth=(config.user, config.password),
    )


def get_es_client(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    """
    builds the elastic search client based on the configuration.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An elasticsearch client
    """
    return elasticsearch.Elasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(
            config.user,
            config.password,
        ),
    )


def get_viz_input_builders(
    old_config: old_config.BaseConfig,
    es_config: es_config.Elasticsearch,
    sql_context: sql.SQLContext,
    es_client: elasticsearch.Elasticsearch,
    es_dataframe_util: es_utils.DataFrameUtil,
    es_rdd_util: es_utils.RDDUtil,
    doc_dataframe_util: indexd_utils.DataFrameUtil,
    case_field_selector: es_utils.CaseFieldSelector,
) -> Mapping[
    build.DataFrame, Union[bases.Builder, base_input_builder.BaseInputBuilder]
]:
    """
    Builds the input builders required for the viz export process.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        es_config: The configurations for connecting to the elasticsearch cluster.
        sql_context: The SQLContext for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        es_rdd_util: A utility for loading rdd objects from the elasticsearch cluster.
        doc_dataframe_util: A utility for reading document data from documents found in
            the indexd store.
        case_field_selector: A utility for loading the required case fields for a given
            index or set of indices.

    Returns:
        A mapping of the build.DataFrame to the builder which will produce said data
        frame.
    """
    annotation_builders = (civic.CivicBuilder(old_config, sql_context),)
    file_filter_factory = maf_metadata.MAFFileFilterFactory(old_config, es_client)
    ascat_doc_resolver = ascat.DocumentResolver(es_config.read, es_client)

    return types.MappingProxyType(
        {
            build.DataFrame.ASCAT: builders.ASCATBuilder(
                old_config,
                sql_context,
                doc_dataframe_util,
                es_dataframe_util,
                ascat_doc_resolver,
            ),
            build.DataFrame.CASE: builders.CaseBuilder(
                old_config, sql_context, es_dataframe_util, case_field_selector
            ),
            build.DataFrame.GENE_MODEL: builders.GeneModelBuilder(
                old_config, sql_context
            ),
            build.DataFrame.MAF: builders.MAFBuilder(
                old_config, sql_context, doc_dataframe_util, annotation_builders
            ),
            build.DataFrame.MAF_METADATA: builders.MAFMetadataBuilder(
                old_config, sql_context, es_dataframe_util, file_filter_factory
            ),
            build.DataFrame.PRIMARY_ALIQUOT: builders.PrimaryAliquotBuilder(
                old_config, sql_context, es_dataframe_util, es_rdd_util
            ),
        }
    )


def get_viz_index_builders(
    old_config: old_config.BaseConfig,
    sql_context: sql.SQLContext,
    es_dataframe_util: es_utils.DataFrameUtil,
    es_rdd_util: es_utils.RDDUtil,
    case_field_selector: es_utils.CaseFieldSelector,
) -> Mapping[build.IndexType, base_builder.BaseBuilder]:
    """
    Builds the index builders required for the viz export process.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        sql_context: The SQLContext for the current spark run.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        es_rdd_util: A utility for loading rdd objects from the elasticsearch cluster.
        case_field_selector: A utility for loading the required case fields for a given
            index or set of indices.

    Returns:
        A mapping of the build.IndexType to the builder which will build and then load
        the said index into es.
    """
    consequence_builder = builders.ConsequenceBuilder()
    observation_builder = builders.ObservationBuilder()

    return types.MappingProxyType(
        {
            build.IndexType.CASE_CENTRIC: builders.CaseCentricBuilder(
                old_config,
                sql_context,
                es_dataframe_util,
                es_rdd_util,
                case_field_selector,
                consequence_builder,
                observation_builder,
            ),
            build.IndexType.CNV_CENTRIC: builders.CNVCentricBuilder(
                old_config, sql_context, consequence_builder, observation_builder
            ),
            build.IndexType.CNV_OCCURRENCE_CENTRIC: builders.CNVOccurrenceCentricBuilder(
                old_config, sql_context, consequence_builder, observation_builder
            ),
            build.IndexType.GENE_CENTRIC: builders.GeneCentricBuilder(
                old_config, sql_context, consequence_builder, observation_builder
            ),
            build.IndexType.SSM_CENTRIC: builders.SSMCentricBuilder(
                old_config, sql_context, consequence_builder, observation_builder
            ),
            build.IndexType.SSM_OCCURRENCE_CENTRIC: builders.SSMOccurrenceCentricBuilder(
                old_config, sql_context, consequence_builder, observation_builder
            ),
        }
    )


def get_viz_builders(
    config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> gdc_mutation_export.Builders:
    """
    Builds the exporters Builders object with the required builders for the viz process.

    Args:
        config: The old master configuration with all subconfigurations for builders and
            services.
        spark_session: The SparkSession for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.

    Returns:
        The Builders object to used by the export process.
    """
    indexd = get_index_client(config.indexd)
    config_adapter = old_config.ConfigAdapter(config, es_client, indexd)
    sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
    es_dataframe_util = es_utils.DataFrameUtil(config_adapter, sql_context, es_client)
    es_rdd_util = es_utils.RDDUtil(config_adapter, spark_session.sparkContext)
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)
    case_field_selector = es_utils.CaseFieldSelector()

    viz_input_builders = get_viz_input_builders(
        config_adapter,
        config.elasticsearch,
        sql_context,
        es_client,
        es_dataframe_util,
        es_rdd_util,
        doc_dataframe_util,
        case_field_selector,
    )
    viz_index_builders = get_viz_index_builders(
        config_adapter, sql_context, es_dataframe_util, es_rdd_util, case_field_selector
    )

    return gdc_mutation_export.Builders(viz_input_builders, viz_index_builders)


def get_ge_input_builders(
    old_config: old_config.BaseConfig,
    sql_context: sql.SQLContext,
    es_dataframe_util: es_utils.DataFrameUtil,
    doc_dataframe_util: indexd_utils.DataFrameUtil,
) -> Mapping[
    build.DataFrame, Union[bases.Builder, base_input_builder.BaseInputBuilder]
]:
    """
    Builds the input builders required for the gene expression export process.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        sql_context: The SQLContext for the current spark run.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        doc_dataframe_util: A utility for reading document data from documents found in
            the indexd store.

    Returns:
        A mapping of the build.DataFrame to the builder which will produce said data
        frame.
    """
    return types.MappingProxyType(
        {
            build.DataFrame.GENE_MODEL: builders.GeneModelBuilder(
                old_config, sql_context
            ),
            build.DataFrame.PRIMARY_ALIQUOT: builders.GeneExpressionPrimaryAliquotBuilder(
                old_config, sql_context, es_dataframe_util
            ),
            build.DataFrame.CASE: builders.GeneExpressionCaseInputBuilder(
                old_config, sql_context
            ),
            build.DataFrame.EXPRESSION_VALUE: builders.GeneExpressionValueInputBuilder(
                old_config, sql_context, doc_dataframe_util
            ),
        }
    )


def get_ge_index_builders(
    old_config: old_config.BaseConfig,
    sql_context: sql.SQLContext,
) -> Mapping[build.IndexType, base_builder.BaseBuilder]:
    """
    Builds the index builders required for the gene expression export process.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        sql_context: The SQLContext for the current spark run.

    Returns:
        A mapping of the build.IndexType to the builder which will build and then load
        the said index into es.
    """
    return types.MappingProxyType(
        {
            build.IndexType.GENE_EXPRESSION: builders.GeneExpressionBuilder(
                old_config, sql_context
            )
        }
    )


def get_ge_builders(
    config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> gdc_mutation_export.Builders:
    """
    Builds the exporters Builders object with the required builders for the viz process.

    Args:
        config: The old master configuration with all subconfigurations for builders and
            services.
        spark_session: The SparkSession for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.

    Returns:
        The Builders object to used by the export process.
    """
    indexd = get_index_client(config.indexd)
    config_adapter = old_config.ConfigAdapter(config, es_client, indexd)
    sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
    es_dataframe_util = es_utils.DataFrameUtil(config_adapter, sql_context, es_client)
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

    return gdc_mutation_export.Builders(
        get_ge_input_builders(
            config_adapter, sql_context, es_dataframe_util, doc_dataframe_util
        ),
        get_ge_index_builders(config_adapter, sql_context),
    )


def main():
    try:
        config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(  # type: ignore
            toml.load("configuration.toml")
        )

        with get_es_client(
            config.elasticsearch.connection
        ) as es_client, initialize_spark() as spark_session:
            if config.build.is_viz_build():
                builders = get_viz_builders(config, spark_session, es_client)
                exporter = gdc_mutation_export.VizExport(
                    spark_session.sparkContext, config.build.index_types, builders
                )
            else:
                builders = get_ge_builders(config, spark_session, es_client)
                exporter = gdc_mutation_export.GEExport(
                    spark_session.sparkContext, config.build.index_types, builders
                )

            exporter.run_export()
    except Exception as ex:
        logger.critical("Driver failed", exc_info=ex)
