import asyncio
import contextlib
import itertools
import logging
from collections.abc import Awaitable, Container, Iterable, Iterator, Mapping
from typing import Optional, cast

import aiohttp
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
from mutation_indexer.configuration import indexd
from mutation_indexer.configuration.builders import gene_expression, viz
from mutation_indexer.constants import build

logger = logging.getLogger("mutation_indexer")


class BuilderAdapter(bases.Builder):
    INPUTS = {
        "case_centric": (
            build.DataFrame.MAF_METADATA,
            build.DataFrame.MAF,
            build.DataFrame.ASCAT,
            build.DataFrame.PRIMARY_ALIQUOT,
        ),
        "cnv_centric": (),
        "cnv_occurrence_centric": (),
        "gene_centric": (),
        "ssm_centric": (),
        "ssm_occurrence_centric": (),
    }
    OUTPUTS = {
        "case_centric": build.DataFrame.CASE_CENTRIC,
        "cnv_centric": build.DataFrame.CNV_CENTRIC,
        "cnv_occurrence_centric": build.DataFrame.CNV_OCCURRENCE_CENTRIC,
        "gene_centric": build.DataFrame.GENE_CENTRIC,
        "ssm_centric": build.DataFrame.SSM_CENTRIC,
        "ssm_occurrence_centric": build.DataFrame.SSM_OCCURRENCE_CENTRIC,
    }

    def __init__(self, builder: base_builder.BaseBuilder) -> None:
        self._builder = builder

    @property
    def inputs(self) -> Iterable[build.DataFrame]:
        return self.INPUTS[self._builder.index_name]

    @property
    def output(self) -> build.DataFrame:
        return self.OUTPUTS[self._builder.index_name]

    def build(self, **inputs: sql.DataFrame) -> Awaitable[sql.DataFrame]:
        return self._builder.build(**inputs)


def _get_required_builders(
    builders: Mapping[build.DataFrame, bases.Builder],
    required: list[bases.Builder],
    inputs: set[build.DataFrame],
) -> Iterator[bases.Builder]:
    builder = required.pop()

    yield builder

    new_inputs = frozenset(builder.inputs) - inputs

    if new_inputs:
        required.extend(builders[d] for d in new_inputs)
        inputs.update(new_inputs)

    if required:
        yield from _get_required_builders(builders, required, inputs)


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


def get_index_client(
    config: indexd.IndexD, connector: aiohttp.BaseConnector
) -> indexd_utils.IndexClient:
    """
    Builds the index client with the given configuration values.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An indexd client
    """
    return indexd_utils.IndexClient(config, connector)


class ESClientResponse(aiohttp.ClientResponse):
    async def text(self, encoding=None, errors="strict"):
        if self._body is None:
            await self.read()

        return self._body.decode("utf-8", "surrogatepass")  # type: ignore


def get_es_client(
    config: es_config.Connection, connector: aiohttp.BaseConnector
) -> elasticsearch.AsyncElasticsearch:
    """
    builds the elastic search client based on the configuration.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An elasticsearch client
    """

    class Connection(elasticsearch.AIOHttpConnection):
        async def _create_aiohttp_session(self):
            if self.loop is None:
                self.loop = asyncio.get_running_loop()
            self.session = aiohttp.ClientSession(
                headers=self.headers,
                skip_auto_headers=("accept", "accept-encoding", "user-agent"),
                auto_decompress=True,
                loop=self.loop,
                cookie_jar=aiohttp.DummyCookieJar(),
                response_class=ESClientResponse,
                connector=connector,
                connector_owner=False,
            )

    return elasticsearch.AsyncElasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(config.user, config.password),
        connection_class=Connection,
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
    index_types: Container[build.IndexType],
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
    consequence_builder = builders.ConsequenceBuilder()
    observation_builder = builders.ObservationBuilder()
    sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
    index_builders: dict[build.IndexType, base_builder.BaseBuilder] = {
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
    adapters = tuple(
        BuilderAdapter(b) for t, b in index_builders.items() if t in index_types
    )
    input_builders: Iterable[bases.Builder] = (
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
            config.maf_metadata,
            spark_session,
            es_dataframe_util,
            file_filter_factory,
        ),
        builders.PrimaryAliquotBuilder(
            config.primary_aliquot, spark_session, es_dataframe_util, es_rdd_util
        ),
    )
    all_builders = {b.output: b for b in itertools.chain(adapters, input_builders)}

    yield from _get_required_builders(all_builders, list(adapters), set())


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

    viz_builders = _get_viz_builders(
        config.builders.viz,
        config_adapter,
        config.elasticsearch,
        spark_session,
        es_client,
        es_dataframe_util,
        es_rdd_util,
        doc_dataframe_util,
        case_field_selector,
        config.build.index_types,
    )

    return tuple(viz_builders)


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
    )

    yield from input_builders

    if build.IndexType.GENE_EXPRESSION in index_types:
        yield builders.GeneExpressionIndexBuilder(
            config.gene_expression,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            doc_dataframe_util,
        )


def get_ge_builders(
    config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.AsyncElasticsearch,
    indexd: indexd_utils.IndexClient,
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
    es_dataframe_util = es_utils.DataFrameUtil(
        config.elasticsearch, spark_session, es_client, es_utils.MappingsLoader()
    )
    doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, spark_session)
    mappings_loader = es_utils.MappingsLoader()

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


async def _main(config: Optional[configuration.Configuration] = None):
    mutation_indexer_logging.configure()

    try:
        config = config or cast(
            configuration.Configuration,
            configuration.CONFIG_SCHEMA.load(  # type: ignore
                toml.load("configuration.toml")
            ),
        )

        mutation_indexer_logging.add_build_id(config.build.build_id)

        async with aiohttp.TCPConnector() as connector, get_es_client(
            config.elasticsearch.connection, connector
        ) as es_client, get_index_client(config.indexd, connector) as indexd:
            with initialize_spark() as spark_session:
                builders = (
                    get_viz_builders(config, spark_session, es_client, indexd)
                    # if config.build.is_viz_build()
                    # else get_ge_builders(config, spark_session, es_client, indexd)
                )
                exporter = gdc_mutation_export.Exporter(
                    spark_session.sparkContext, builders
                )

                await exporter.run()
    except Exception as ex:
        logger.critical("Driver failed", exc_info=ex)


def main():
    asyncio.run(_main())
