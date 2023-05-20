import graphlib
import logging
from collections.abc import Iterable, Mapping, MutableMapping
from typing import NamedTuple

import pyspark
from pyspark import sql

import config
from exports.builders import base_builder, bases
from exports.constants import build

logging.basicConfig(format=config.LOG_FORMAT)

logger = logging.getLogger(__name__)


class Builders(NamedTuple):
    builders: Iterable[bases.Builder]
    index_builders: Mapping[build.IndexType, base_builder.BaseBuilder]


class Exporter:
    """
    The main entry point into the index export process for the mutation indices
    """

    __slots__ = ("_spark_context", "_builders", "_index_types", "_index_builders")

    def __init__(
        self,
        spark_context: pyspark.SparkContext,
        index_types: Iterable[build.IndexType],
        builders: Builders,
    ) -> None:
        self._spark_context = spark_context
        self._builders: Iterable[bases.Builder] = graphlib.TopologicalSorter(
            (b, b.inputs) for b in builders.builders
        ).static_order()

        # TODO: Remove when old index builders ported to new base.
        self._index_types = frozenset(index_types) & builders.index_builders.keys()
        self._index_builders = builders.index_builders

    def run(self) -> None:
        """
        Executes the export for the configured data by running the required builders.
        """
        inputs: MutableMapping[str, sql.DataFrame] = {}

        for builder in self._builders:
            output = builder.output

            self._spark_context.setJobGroup(output.name, f"Build {output.name}")

            inputs[output.to_param()] = builder.build(**inputs)

        # TODO: Remove when old index builders ported to new base
        for index_type in self._index_types:
            if index_type not in self._index_builders:
                raise ValueError(f"No builder is configured for index: {index_type}.")

            self._spark_context.setJobGroup(index_type.name, f"Build {index_type}")
            self._index_builders[index_type].build(**inputs).load()

        logger.info("Mutation Indexer finished successfully")
