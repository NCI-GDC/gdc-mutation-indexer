import logging
from collections.abc import Iterable

import elasticsearch
from indexclient import client
from pyspark import sql

from mutation_indexer import driver, es_utils, indexd_utils
from mutation_indexer.builders import bases, gene_model
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import builders, configuration

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
        sql_context = sql.SQLContext(spark_session.sparkContext, spark_session)
        es_dataframe_util = es_utils.DataFrameUtil(
            config.elasticsearch, spark_session, es_client, es_utils.MappingsLoader()
        )
        doc_dataframe_util = indexd_utils.DataFrameUtil(indexd, sql_context, logger)
        mappings_loader = es_utils.MappingsLoader()
        input_builders = (
            gene_model.GeneModelBuilder(config.builders.gene_model, spark_session),
            builders.PrimaryAliquotBuilder(
                config.builders.primary_aliquot, spark_session, es_dataframe_util
            ),
            builders.CaseBuilder(config.builders.case, spark_session),
            builders.ExpressionValueBuilder(
                config.builders.expression_value, spark_session, doc_dataframe_util
            ),
        )

        yield from input_builders

        if build.IndexType.GENE_EXPRESSION in config.build.index_types:
            yield builders.GeneExpressionBuilder(
                config.builders.gene_expression,
                spark_session,
                es_dataframe_util,
                mappings_loader,
            )
