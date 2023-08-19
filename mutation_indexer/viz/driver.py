import logging
import types
from collections.abc import Iterable, Mapping

import elasticsearch
from indexclient import client
from pyspark import sql

from mutation_indexer import driver, es_utils, indexd_utils
from mutation_indexer.builders import base_builder, bases, gene_model
from mutation_indexer.configuration import old_adapter
from mutation_indexer.viz import builders, configuration, constants
from mutation_indexer.viz.builders import (
    ascat,
    case,
    consequence,
    maf_metadata,
    observation,
)
from mutation_indexer.viz.builders.clinical_annotations import civic
from mutation_indexer.viz.configuration import Configuration

logger = logging.getLogger(__name__)


class Driver(driver.Driver[configuration.Configuration]):
    def __init__(self) -> None:
        super().__init__(configuration.SCHEMA)

    def _get_builders(
        self,
        config: configuration.Configuration,
        spark_session: sql.SparkSession,
        es_client: elasticsearch.Elasticsearch,
        indexd: client.IndexClient,
    ) -> Iterable[bases.Builder]:
        config_adapter = old_adapter.BaseConfig(config, es_client, indexd)
        sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
        doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)
        annotation_builders = (civic.CivicBuilder(config_adapter, sql_context),)
        file_filter_factory = maf_metadata.MAFFileFilterFactory(
            config.elasticsearch.read, es_client
        )
        ascat_doc_resolver = ascat.DocumentResolver(
            config.elasticsearch.read, es_client
        )
        mappings_loader = es_utils.MappingsLoader()
        es_dataframe_util = es_utils.DataFrameUtil(
            config.elasticsearch, spark_session, es_client, mappings_loader
        )
        es_rdd_util = es_utils.RDDUtil(config.elasticsearch, spark_session.sparkContext)
        case_field_selector = case.CaseFieldSelector(mappings_loader)

        input_builders = (
            builders.ASCATBuilder(
                config.builders.ascat,
                spark_session,
                doc_dataframe_util,
                es_dataframe_util,
                ascat_doc_resolver,
            ),
            builders.CaseBuilder(
                config.builders.case, spark_session, es_dataframe_util, case_field_selector
            ),
            gene_model.GeneModelBuilder(config.builders.gene_model, spark_session),
            builders.MAFBuilder(
                config.builders.maf, spark_session, doc_dataframe_util, annotation_builders
            ),
            builders.MAFMetadataBuilder(
                config.builders.maf_metadata,
                spark_session,
                es_dataframe_util,
                file_filter_factory,
            ),
            builders.PrimaryAliquotBuilder(
                config.builders.primary_aliquot, spark_session, es_dataframe_util, es_rdd_util
            ),
        )

        yield from input_builders

    def _get_index_builders(self, config: Configuration, spark_session: sql.SparkSession, es_client: elasticsearch.Elasticsearch, indexd: client.IndexClient) -> Mapping[build.IndexType, base_builder.BaseBuilder]:
        mappings_loader = es_utils.MappingsLoader()
        config_adapter = old_adapter.BaseConfig(config, es_client, indexd)
        sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
        es_dataframe_util = es_utils.DataFrameUtil(
            config.elasticsearch, spark_session, es_client, mappings_loader
        )
        es_rdd_util = es_utils.RDDUtil(config.elasticsearch, spark_session.sparkContext)
        case_field_selector = case.CaseFieldSelector(mappings_loader)
        consequence_builder = consequence.ConsequenceBuilder()
        observation_builder = observation.ObservationBuilder()

        return types.MappingProxyType(
            {
                constants.IndexType.CASE_CENTRIC: builders.CaseCentricBuilder(
                    config_adapter,
                    sql_context,
                    es_dataframe_util,
                    es_rdd_util,
                    case_field_selector,
                    consequence_builder,
                    observation_builder,
                ),
                constants.IndexType.CNV_CENTRIC: builders.CNVCentricBuilder(
                    config_adapter, sql_context, consequence_builder, observation_builder
                ),
                constants.IndexType.CNV_OCCURRENCE_CENTRIC: builders.CNVOccurrenceCentricBuilder(
                    config_adapter, sql_context, consequence_builder, observation_builder
                ),
                constants.IndexType.GENE_CENTRIC: builders.GeneCentricBuilder(
                    config_adapter, sql_context, consequence_builder, observation_builder
                ),
                constants.IndexType.SSM_CENTRIC: builders.SSMCentricBuilder(
                    config_adapter, sql_context, consequence_builder, observation_builder
                ),
                constants.IndexType.SSM_OCCURRENCE_CENTRIC: builders.SSMOccurrenceCentricBuilder(
                    config_adapter, sql_context, consequence_builder, observation_builder
                ),
            }
        )
