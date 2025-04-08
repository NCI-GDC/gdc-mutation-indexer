import contextlib
import logging
import types
from collections.abc import Container, Iterator, Mapping

import elasticsearch
import toml
from indexclient import client
from pyspark import sql

from mutation_indexer import (
    builders,
    configuration,
    es_utils,
    gdc_mutation_export,
    indexd_utils,
)
from mutation_indexer import logging as mutation_indexer_logging
from mutation_indexer.builders import base_builder, bases, civic, maf_metadata
from mutation_indexer.configuration import adapter
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.configuration import indexd
from mutation_indexer.configuration.builders import gene_expression, viz
from mutation_indexer.constants import build

logger = logging.getLogger("mutation_indexer")


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


def _get_viz_builders(
    config: viz.Viz,
    es_config: es_config.Elasticsearch,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    es_dataframe_util: es_utils.DataFrameUtil,
    es_rdd_util: es_utils.RDDUtil,
    doc_dataframe_util: indexd_utils.DataFrameUtil,
    case_field_selector: es_utils.CaseFieldSelector,
    index_types: Container[build.IndexType],
    mappings_loader: es_utils.MappingsLoader,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
) -> Iterator[bases.Builder]:
    """
    Builds the input builders required for the viz export process.

    Args:
        config: The configuration for the builder objects.
        es_config: The configurations for connecting to the elasticsearch cluster.
        spark_session: The SparkSession for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        es_rdd_util: A utility for loading rdd objects from the elasticsearch cluster.
        doc_dataframe_util: A utility for reading document data from documents found in
            the indexd store.
        case_field_selector: A utility for loading the required case fields for a given
            index or set of indices.
        index_types: A container of all required index types for this build.
        mappings_loader: The mapping loader service for loading ES mappings.
        consequence_builder: The builder service for loading consequence data.
        observation_builder: The builder service for loading observation data.

    Returns:
        An iterator of all the builders required for the build.
    """
    file_filter_factory = maf_metadata.MAFFileFilterFactory(es_config.read, es_client)

    input_builders = (
        builders.ASCATMetadataBuilder(
            config.ascat_metadata, spark_session, es_dataframe_util, es_rdd_util
        ),
        builders.ASCATBuilder(config.ascat, spark_session, doc_dataframe_util),
        builders.CaseBuilder(
            config.case, spark_session, es_dataframe_util, case_field_selector
        ),
        civic.DNABuilder(config.civic_dna, spark_session),
        civic.ProteinBuilder(config.civic_protein, spark_session),
        builders.GeneModelBuilder(config.gene_model, spark_session),
        builders.MAFBuilder(config.maf, spark_session, doc_dataframe_util),
        builders.MAFMetadataBuilder(
            config.maf_metadata, spark_session, es_dataframe_util, file_filter_factory
        ),
        builders.PrimaryAliquotBuilder(
            config.primary_aliquot, spark_session, es_dataframe_util, es_rdd_util
        ),
        builders.SegmentCNVBuilder(
            config.segment_cnv, spark_session, doc_dataframe_util
        ),
        builders.SegmentCNVMetadataBuilder(
            config.segment_cnv_metadata, spark_session, es_dataframe_util
        ),
    )
    yield from input_builders

    if build.IndexType.SEGMENT_CNV_CENTRIC in index_types:
        yield builders.SegmentCNVCentricBuilder(
            config.segment_cnv_centric,
            spark_session,
            es_dataframe_util,
            mappings_loader,
        )

    if build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC in index_types:
        yield builders.SegmentCNVOccurrenceCentricBuilder(
            config.segment_cnv_occurrence_centric,
            spark_session,
            es_dataframe_util,
            mappings_loader,
        )


def get_obsolete_viz_index_builders(
    old_config: adapter.ObsoleteConfig,
    sql_context: sql.SQLContext,
    es_dataframe_util: es_utils.DataFrameUtil,
    es_rdd_util: es_utils.RDDUtil,
    case_field_selector: es_utils.CaseFieldSelector,
    consequence_builder: builders.ConsequenceBuilder,
    observation_builder: builders.ObservationBuilder,
) -> Mapping[build.IndexType, base_builder.BaseBuilder]:
    """Builds the index builders required for the viz export process.

    NOTE: this function currently returns all the index builders that inherit from
    the BaseBuilder class. The goal is to transition these index builders to follow
    the Builder protocol, and then move the instantiation of these index builders
    to the _get_viz_builders() function.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        sql_context: The SQLContext for the current spark run.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        es_rdd_util: A utility for loading rdd objects from the elasticsearch cluster.
        case_field_selector: A utility for loading the required case fields for a given
            index or set of indices.
        consequence_builder: The builder service for loading consequence data.
        observation_builder: The builder service for loading observation data.

    Returns:
        A mapping of the build.IndexType to the builder which will build and then load
        the said index into es.
    """

    return types.MappingProxyType(
        {
            build.IndexType.CASE_CENTRIC: builders.CaseCentricBuilder(
                old_config,
                sql_context,
                es_dataframe_util,
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
        config: The master configuration with all sub-configurations for builders and
            services.
        spark_session: The SparkSession for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.

    Returns:
        The Builders object to used by the export process.
    """
    mappings_loader = es_utils.MappingsLoader()
    indexd = get_index_client(config.indexd)
    config_adapter = adapter.ObsoleteConfig(config, es_client, indexd)
    sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
    es_dataframe_util = es_utils.DataFrameUtil(
        config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    es_rdd_util = es_utils.RDDUtil(config.elasticsearch, spark_session.sparkContext)
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)
    case_field_selector = es_utils.CaseFieldSelector(mappings_loader)
    consequence_builder = builders.ConsequenceBuilder()
    observation_builder = builders.ObservationBuilder()

    viz_builders = _get_viz_builders(
        config.builders.viz,
        config.elasticsearch,
        spark_session,
        es_client,
        es_dataframe_util,
        es_rdd_util,
        doc_dataframe_util,
        case_field_selector,
        config.build.index_types,
        mappings_loader,
        consequence_builder,
        observation_builder,
    )
    viz_index_builders = get_obsolete_viz_index_builders(
        config_adapter,
        sql_context,
        es_dataframe_util,
        es_rdd_util,
        case_field_selector,
        consequence_builder,
        observation_builder,
    )

    return gdc_mutation_export.Builders(tuple(viz_builders), viz_index_builders)


def _get_ge_builders(
    config: gene_expression.GeneExpression,
    spark_session: sql.SparkSession,
    es_dataframe_util: es_utils.DataFrameUtil,
    doc_dataframe_util: indexd_utils.DataFrameUtil,
    index_types: Container[build.IndexType],
    mappings_loader: es_utils.MappingsLoader,
) -> Iterator[bases.Builder]:
    """
    Builds the input builders required for the gene expression export process.

    Args:
        old_config: The old god configuration object with all of the configuration
            values needed to run any and all builders.
        config: The configuration for the gene expression builders.
        sql_context: The SQLContext for the current spark run.
        spark_session: The SparkSession for the current spark run.
        es_dataframe_util: A utility for loading and writing data frames to and from
            elasticsearch to be used by the builders.
        doc_dataframe_util: A utility for reading document data from documents found in
            the indexd store.
        index_types: A container of all required index types for this build.
        mappings_loader: The mapping loader service for loading ES mappings.

    Returns:
        An iterator of the Builder objects required for this build.
    """
    input_builders = (
        builders.GeneModelBuilder(config.gene_model, spark_session),
        builders.GeneExpressionPrimaryAliquotBuilder(
            config.primary_aliquot, spark_session, es_dataframe_util
        ),
        builders.ExpressionValueBuilder(
            config.expression_value, spark_session, doc_dataframe_util
        ),
    )

    yield from input_builders

    if build.IndexType.GENE_EXPRESSION in index_types:
        yield builders.GeneExpressionIndexBuilder(
            config.gene_expression,
            spark_session,
            es_dataframe_util,
            mappings_loader,
        )


def get_ge_builders(
    config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> gdc_mutation_export.Builders:
    """
    Builds the exporters Builders object with the required builders for the viz process.

    Args:
        config: The master configuration with all sub-configurations for builders and
            services.
        spark_session: The SparkSession for the current spark run.
        es_client: The client for interacting with the elasticsearch cluster.

    Returns:
        The Builders object to used by the export process.
    """
    indexd = get_index_client(config.indexd)
    sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)

    return gdc_mutation_export.Builders(
        tuple(
            _get_ge_builders(
                config.builders.gene_expression,
                spark_session,
                es_dataframe_util,
                doc_dataframe_util,
                config.build.index_types,
                mappings_loader,
            )
        ),
        {},
    )


def main():
    mutation_indexer_logging.configure()

    try:
        config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(  # type: ignore
            toml.load("configuration.toml")
        )

        mutation_indexer_logging.add_build_id(config.build.build_id)

        with (
            es_utils.initialize_client(config.elasticsearch.connection) as es_client,
            initialize_spark() as spark_session,
        ):
            builders = (
                get_viz_builders(config, spark_session, es_client)
                if config.build.is_viz_build()
                else get_ge_builders(config, spark_session, es_client)
            )
            exporter = gdc_mutation_export.Exporter(
                spark_session.sparkContext, config.build.index_types, builders
            )

            exporter.run()
    except Exception as ex:
        logger.critical("Driver failed", exc_info=ex)
