"""This module is for implementing how the viz driver will be executed."""

import contextlib
import logging
from collections.abc import Container, Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

from pyspark import sql

from mutation_indexer import configuration, driver, es_utils, indexd_utils
from mutation_indexer.builders import bases
from mutation_indexer.builders import gene_expression as builders
from mutation_indexer.builders import gene_model
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build


class Dependencies(NamedTuple):
    doc_dataframe_util: indexd_utils.DataFrameUtil
    es_dataframe_util: es_utils.DataFrameUtil
    mappings_loader: es_utils.MappingsLoader
    spark_session: sql.SparkSession


class Driver(driver.Driver[configuration.Configuration]):
    @classmethod
    def load_config(cls, file: Path) -> configuration.Configuration:
        return configuration.Configuration.load(file)

    @contextlib.contextmanager
    def _initialize_dependencies(
        self, config: configuration.Configuration, spark_session: sql.SparkSession
    ) -> Iterator[Dependencies]:
        """Initializes all dependencies upon which the builders depend.

        Args:
            config: The configuration for this run of the driver.
            spark_session: The spark session for this run of the driver.

        Returns:
            A context manager wrapping the dependencies. The context should be exited
            only after the dependent builders and done being used.
        """
        with driver.get_es_client(config.elasticsearch.connection) as es_client:
            index_client = driver.get_index_client(config.indexd)
            mappings_loader = es_utils.MappingsLoader()

            yield Dependencies(
                indexd_utils.DataFrameUtil(
                    index_client,
                    sql.SQLContext(spark_session.sparkContext, spark_session),
                    logging.getLogger(indexd_utils.__name__),
                ),
                es_utils.DataFrameUtil(
                    config.elasticsearch,
                    spark_session,
                    es_client,
                    mappings_loader,
                    es_utils.SchemaLoader(),
                ),
                mappings_loader,
                spark_session,
            )

    def _builders(
        self,
        config: gene_expression.GeneExpression,
        index_types: Container[build.IndexType],
        dependencies: Dependencies,
    ) -> Iterator[bases.Builder]:
        """Gets all builders associated with the viz driver & used by other builders.

        Args:
            config: The configuration for builders in this run of the driver.
            dependencies: The dependencies for the builders.

        Return:
            An iterable of all gene expression builders.
        """
        yield gene_model.GeneModelBuilder(config.gene_model, dependencies.spark_session)
        yield builders.PrimaryAliquotBuilder(
            config.primary_aliquot,
            dependencies.spark_session,
            dependencies.es_dataframe_util,
        )
        yield builders.ExpressionValueBuilder(
            config.expression_value,
            dependencies.spark_session,
            dependencies.doc_dataframe_util,
        )

        if build.IndexType.GENE_EXPRESSION in index_types:
            yield builders.IndexBuilder(
                config.gene_expression,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
            )

    @contextlib.contextmanager
    def _initialize_builders(
        self, config: configuration.Configuration, spark_session: sql.SparkSession
    ) -> Iterator[Iterable[bases.Builder]]:
        with self._initialize_dependencies(config, spark_session) as dependencies:
            yield tuple(
                self._builders(
                    config.builders.gene_expression,
                    config.build.index_types,
                    dependencies,
                )
            )


def main() -> None:
    """The main functionality for this driver module."""
    driver.main(Driver())
