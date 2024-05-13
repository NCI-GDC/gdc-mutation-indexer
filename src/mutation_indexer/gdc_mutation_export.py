import asyncio
import logging
from collections.abc import Iterable, Mapping
from typing import NamedTuple

import more_itertools
import pyspark
from pyspark import sql

from mutation_indexer.builders import base_builder, bases
from mutation_indexer.constants import app, build

logging.basicConfig(format=app.LOG_FORMAT)

logger = logging.getLogger(__name__)


class Builders(NamedTuple):
    builders: Iterable[bases.Builder]
    index_builders: Mapping[build.IndexType, base_builder.BaseBuilder]


class Graph:
    class Node:
        __slots__ = ("_builder", "_input_queue", "_output_queues")

        def __init__(
            self,
            builder: bases.Builder,
            input_queue: asyncio.Queue[tuple[build.DataFrame, sql.DataFrame]],
            output_queues: Iterable[
                asyncio.Queue[tuple[build.DataFrame, sql.DataFrame]]
            ],
        ) -> None:
            self._builder = builder
            self._input_queue = input_queue
            self._output_queues = output_queues

        async def run(self) -> None:
            inputs: dict[str, sql.DataFrame] = {}
            required_inputs = frozenset(self._builder.inputs)

            while not inputs.keys() == required_inputs:
                print(f"{self._builder.output.name} waiting")
                input_name, df = await self._input_queue.get()
                print(f"{self._builder.output.name} received: {input_name.name}")
                inputs[input_name.to_param()] = df

            print(f"Building: {self._builder.output.name}")
            output = await self._builder.build(**inputs)
            print(f"DONE: {self._builder.output.name}")

            for queue in self._output_queues:
                print(f"Putting: {self._builder.output.name}")
                await queue.put((self._builder.output, output))

    __slots__ = ("_nodes",)

    def __init__(self, nodes: Iterable[Node]) -> None:
        self._nodes = nodes

    @staticmethod
    def load(builders: Iterable[bases.Builder]) -> "Graph":
        input_queues: Mapping[
            build.DataFrame, asyncio.Queue[tuple[build.DataFrame, sql.DataFrame]]
        ] = {b.output: asyncio.Queue() for b in builders}
        output_queues = more_itertools.map_reduce(
            ((i, b) for b in builders for i in b.inputs),
            keyfunc=lambda i: i[0],
            valuefunc=lambda i: input_queues[i[1].output],
        )

        def get_node(builder: bases.Builder) -> Graph.Node:
            return Graph.Node(
                builder, input_queues[builder.output], output_queues[builder.output]
            )

        nodes = tuple(map(get_node, builders))

        print(nodes)

        return Graph(nodes)

    async def run(self) -> None:
        tasks = tuple(asyncio.create_task(node.run()) for node in self._nodes)

        try:
            await asyncio.gather(*tasks)
        except:
            for task in tasks:
                task.cancel()

            raise


class Exporter:
    """
    The main entry point into the index export process for the mutation indices
    """

    __slots__ = ("_spark_context", "_builders")

    def __init__(
        self, spark_context: pyspark.SparkContext, builders: Iterable[bases.Builder]
    ) -> None:
        self._spark_context = spark_context
        self._builders = builders

    async def run(self) -> None:
        """
        Executes the export for the configured data by running the required builders.
        """
        print(f"Running: {self._builders}")
        await Graph.load(self._builders).run()
