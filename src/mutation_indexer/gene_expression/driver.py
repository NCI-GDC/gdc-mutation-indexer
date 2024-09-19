import contextlib
from collections.abc import Iterable, Iterator
from typing import NamedTuple

import elasticsearch
from pyspark import sql

from mutation_indexer import builders, configuration, driver, es_utils, indexd_utils
from mutation_indexer.builders import bases


class Dependencies(NamedTuple):
    spark_session: sql.SparkSession
    es_client: elasticsearch.Elasticsearch
    es_dataframe_util: es_utils.DataFrameUtil
    doc_dataframe_util: indexd_utils.DataFrameUtil
    mappings_loader: es_utils.MappingsLoader


class Driver(driver.Driver):
    @contextlib.contextmanager
    @classmethod
    def _load_dependencies(
        cls, config: configuration.Configuration
    ) -> Iterator[Dependencies]:
        """Loads all dependencies required by builders associated with this driver.

        Args:
            config: The configuration for this given run of the driver.

        Returns:
            A context manager in which all dependencies for the builders can be found.
        """
        mappings_loader = es_utils.MappingsLoader()
        schema_loader = es_utils.SchemaLoader()

        with contextlib.ExitStack() as stack:
            es_client = stack.enter_context(
                cls.load_es_client(config.elasticsearch.connection)
            )
            spark_session = stack.enter_context(cls.load_spark_session())
            es_dataframe_util = es_utils.DataFrameUtil(
                config.elasticsearch,
                spark_session,
                es_client,
                mappings_loader,
                schema_loader,
            )
            doc_dataframe_util = indexd_utils.DataFrameUtil(
                cls.load_index_client(config.indexd), spark_session
            )

            yield Dependencies(
                spark_session,
                es_client,
                es_dataframe_util,
                doc_dataframe_util,
                mappings_loader,
            )

    @contextlib.contextmanager
    @classmethod
    def _load_builders(
        cls, config: configuration.Configuration
    ) -> driver.Iterator[Iterable[bases.Builder]]:
        ge_config = config.builders.gene_expression

        with cls._load_dependencies(config) as deps:
            yield (
                builders.GeneModelBuilder(ge_config.gene_model, deps.spark_session),
                builders.GeneExpressionPrimaryAliquotBuilder(
                    ge_config.primary_aliquot,
                    deps.spark_session,
                    deps.es_dataframe_util,
                ),
                builders.GeneExpressionIndexBuilder(
                    ge_config.gene_expression,
                    deps.spark_session,
                    deps.es_dataframe_util,
                    deps.mappings_loader,
                    deps.doc_dataframe_util,
                ),
            )
