import asyncio
import contextlib
import logging
from collections.abc import Iterable, Iterator

import elasticsearch
import toml
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
from mutation_indexer.configuration.builders import viz

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


def get_es_client(config: es_config.Connection) -> elasticsearch.AsyncElasticsearch:
    """
    builds the elastic search client based on the configuration.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An elasticsearch client
    """

    return elasticsearch.AsyncElasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(config.user, config.password),
        sniff_on_start=True,
        sniff_on_connection_fail=True,
        sniffer_timeout=60,
    )


def _get_viz_builders(
    config: viz.Viz,
    old_config: adapter.ObsoleteConfig,
    es_config: es_config.Elasticsearch,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.AsyncElasticsearch,
    es_dataframe_util: es_utils.DataFrameUtil,
    es_rdd_util: es_utils.RDDUtil,
    doc_dataframe_util: indexd_utils.DataFrameUtil,
    case_field_selector: es_utils.CaseFieldSelector,
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
    old_builders = (
        builders.CaseCentricBuilder(
            old_config,
            spark_session,
            es_dataframe_util,
            es_rdd_util,
            case_field_selector,
            consequence_builder,
            observation_builder,
        ),
        builders.CNVCentricBuilder(
            old_config, spark_session, consequence_builder, observation_builder
        ),
        builders.CNVOccurrenceCentricBuilder(
            old_config, spark_session, consequence_builder, observation_builder
        ),
        builders.GeneCentricBuilder(
            old_config, spark_session, consequence_builder, observation_builder
        ),
        builders.SSMCentricBuilder(
            old_config, spark_session, consequence_builder, observation_builder
        ),
        builders.SSMOccurrenceCentricBuilder(
            old_config, spark_session, consequence_builder, observation_builder
        ),
    )

    yield from map(base_builder.BuilderAdapter, old_builders)

    yield builders.ASCATMetadataBuilder(
        config.ascat_metadata, spark_session, es_dataframe_util, es_rdd_util
    )
    yield builders.ASCATBuilder(config.ascat, spark_session, doc_dataframe_util)
    yield builders.CaseBuilder(
        config.case, spark_session, es_dataframe_util, case_field_selector
    )
    yield civic.DNABuilder(config.civic_dna, spark_session)
    yield civic.ProteinBuilder(config.civic_protein, spark_session)
    yield builders.GeneModelBuilder(config.gene_model, spark_session)
    yield builders.MAFBuilder(config.maf, spark_session, doc_dataframe_util)
    yield builders.MAFMetadataBuilder(
        config.maf_metadata, spark_session, es_dataframe_util, file_filter_factory
    )
    yield builders.PrimaryAliquotBuilder(
        config.primary_aliquot, spark_session, es_dataframe_util, es_rdd_util
    )


def get_viz_builders(
    config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.AsyncElasticsearch,
    indexd: indexd_utils.IndexClient,
) -> Iterable[bases.Builder]:
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
    config_adapter = adapter.ObsoleteConfig(config, es_client)
    es_dataframe_util = es_utils.DataFrameUtil(
        config.elasticsearch, spark_session, es_client, mappings_loader
    )
    es_rdd_util = es_utils.RDDUtil(config.elasticsearch, spark_session.sparkContext)
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, spark_session)
    case_field_selector = es_utils.CaseFieldSelector(mappings_loader)
    consequence_builder = builders.ConsequenceBuilder()
    observation_builder = builders.ObservationBuilder()

    return tuple(
        _get_viz_builders(
            config.builders.viz,
            config_adapter,
            config.elasticsearch,
            spark_session,
            es_client,
            es_dataframe_util,
            es_rdd_util,
            doc_dataframe_util,
            case_field_selector,
            consequence_builder,
            observation_builder,
        )
    )


# def _get_ge_builders(
#     config: gene_expression.GeneExpression,
#     spark_session: sql.SparkSession,
#     es_dataframe_util: es_utils.DataFrameUtil,
#     doc_dataframe_util: indexd_utils.DataFrameUtil,
#     index_types: Container[build.IndexType],
#     mappings_loader: es_utils.MappingsLoader,
# ) -> Iterator[bases.Builder]:
#     """
#     Builds the input builders required for the gene expression export process.

#     Args:
#         old_config: The old god configuration object with all of the configuration
#             values needed to run any and all builders.
#         config: The configuration for the gene expression builders.
#         sql_context: The SQLContext for the current spark run.
#         spark_session: The SparkSession for the current spark run.
#         es_dataframe_util: A utility for loading and writing data frames to and from
#             elasticsearch to be used by the builders.
#         doc_dataframe_util: A utility for reading document data from documents found in
#             the indexd store.
#         index_types: A container of all required index types for this build.
#         mappings_loader: The mapping loader service for loading ES mappings.

#     Returns:
#         An iterator of the Builder objects required for this build.
#     """
#     input_builders = (
#         builders.GeneModelBuilder(config.gene_model, spark_session),
#         builders.GeneExpressionPrimaryAliquotBuilder(
#             config.primary_aliquot, spark_session, es_dataframe_util
#         ),
#     )

#     yield from input_builders

#     if build.IndexType.GENE_EXPRESSION in index_types:
#         yield builders.GeneExpressionIndexBuilder(
#             config.gene_expression,
#             spark_session,
#             es_dataframe_util,
#             mappings_loader,
#             doc_dataframe_util,
#         )


# def get_ge_builders(
#     config: configuration.Configuration,
#     spark_session: sql.SparkSession,
#     es_client: elasticsearch.Elasticsearch,
# ) -> gdc_mutation_export.Builders:
#     """
#     Builds the exporters Builders object with the required builders for the viz process.

#     Args:
#         config: The master configuration with all sub-configurations for builders and
#             services.
#         spark_session: The SparkSession for the current spark run.
#         es_client: The client for interacting with the elasticsearch cluster.

#     Returns:
#         The Builders object to used by the export process.
#     """
#     indexd = get_index_client(config.indexd)
#     sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
#     es_dataframe_util = es_utils.DataFrameUtil(
#         config.elasticsearch, spark_session, es_client, es_utils.MappingsLoader()
#     )
#     doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)
#     mappings_loader = es_utils.MappingsLoader()

#     return gdc_mutation_export.Builders(
#         tuple(
#             _get_ge_builders(
#                 config.builders.gene_expression,
#                 spark_session,
#                 es_dataframe_util,
#                 doc_dataframe_util,
#                 config.build.index_types,
#                 mappings_loader,
#             )
#         ),
#         {},
#     )


async def _main() -> None:
    config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(  # type: ignore
        toml.load("configuration.toml")
    )

    mutation_indexer_logging.add_build_id(config.build.build_id)

    with initialize_spark() as spark_session:
        async with get_es_client(
            config.elasticsearch.connection
        ) as es_client, indexd_utils.IndexClient(config.indexd) as indexd:
            builders = get_viz_builders(config, spark_session, es_client, indexd)
            exporter = gdc_mutation_export.Exporter(
                spark_session.sparkContext, builders
            )

            await exporter.run()


def main() -> None:
    try:
        mutation_indexer_logging.configure()
        asyncio.run(_main())
    except Exception as ex:
        logger.critical("Driver failed", exc_info=ex)
