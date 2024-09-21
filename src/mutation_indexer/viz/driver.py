"""A module containing the Driver which runs the build of the viz indices."""

import contextlib
import inspect
import itertools
from collections.abc import Iterable, Iterator
from typing import NamedTuple, cast

import elasticsearch
import networkx as nx
from pyspark import sql

from mutation_indexer import configuration, driver, es_utils, indexd_utils
from mutation_indexer.configuration import adapter
from mutation_indexer.constants import build
from mutation_indexer.viz import builders
from mutation_indexer.viz.builders import maf_metadata


class BaseBuilderAdapter(builders.Builder):
    """An adapter class allowing a BaseBuilder to be used as a Builder."""

    __slots__ = ("_builder",)

    def __init__(self, builder: builders.BaseBuilder) -> None:
        self._builder = builder

    @property
    def output(self) -> build.DataFrame:
        return build.DataFrame[self._builder.index_name.upper()]

    @property
    def index(self) -> build.IndexType:
        return build.IndexType[self._builder.index_name.upper()]

    @property
    def inputs(self) -> Iterable[build.DataFrame]:
        return tuple(
            build.DataFrame.from_param(p)
            for p in inspect.signature(self._builder.build).parameters.keys()
            if p != "kwargs"
        )

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        self._builder.build(**inputs).load()

        return getattr(self._builder, self._builder.index_name)


class Dependencies(NamedTuple):
    spark_session: sql.SparkSession
    es_client: elasticsearch.Elasticsearch
    es_dataframe_util: es_utils.DataFrameUtil
    es_rdd_util: es_utils.RDDUtil
    doc_dataframe_util: indexd_utils.DataFrameUtil
    case_field_selector: es_utils.CaseFieldSelector
    consequence_builder: builders.ConsequenceBuilder
    observation_builder: builders.ObservationBuilder


def _load_input_builders(
    config: configuration.Configuration, deps: Dependencies
) -> Iterable[builders.Builder]:
    """Loads all input builders associated with the viz driver.

    Args:
        config: The configuration associated with the current run.
        deps: The dependencies of the builders.

    Returns:
        An iterable of instances of the input builders.
    """
    viz = config.builders.viz
    spark_session = deps.spark_session
    es_dataframe_util = deps.es_dataframe_util
    es_rdd_util = deps.es_rdd_util
    doc_dataframe_util = deps.doc_dataframe_util
    case_field_selector = deps.case_field_selector
    file_filter_factory = maf_metadata.MAFFileFilterFactory(
        config.elasticsearch.read, deps.es_client
    )

    return (
        builders.ASCATMetadataBuilder(
            viz.ascat_metadata, spark_session, es_dataframe_util, es_rdd_util
        ),
        builders.ASCATBuilder(viz.ascat, spark_session, doc_dataframe_util),
        builders.CaseBuilder(
            viz.case, spark_session, es_dataframe_util, case_field_selector
        ),
        builders.DNABuilder(viz.civic_dna, spark_session),
        builders.ProteinBuilder(viz.civic_protein, spark_session),
        builders.GeneModelBuilder(viz.gene_model, spark_session),
        builders.MAFBuilder(viz.maf, spark_session, doc_dataframe_util),
        builders.MAFMetadataBuilder(
            viz.maf_metadata, spark_session, es_dataframe_util, file_filter_factory
        ),
        builders.PrimaryAliquotBuilder(
            viz.primary_aliquot, spark_session, es_dataframe_util, es_rdd_util
        ),
    )


def _load_index_builders(
    config: configuration.Configuration, deps: Dependencies
) -> Iterable[builders.Builder]:
    """Loads all the index builders configured for this run the the viz driver.

    Args:
        config: The configuration associated with this run.
        deps: The dependencies of the builders.

    Returns:
        An iterable of all the configured index builders.
    """
    indices = config.build.index_types
    sql_context = cast(sql.SQLContext, deps.spark_session)
    old_config = adapter.ObsoleteConfig(config, deps.es_client)
    es_dataframe_util = deps.es_dataframe_util
    case_field_selector = deps.case_field_selector
    consequence_builder = deps.consequence_builder
    observation_builder = deps.observation_builder

    old_builders = (
        builders.CaseCentricBuilder(
            old_config,
            sql_context,
            es_dataframe_util,
            case_field_selector,
            consequence_builder,
            observation_builder,
        ),
        builders.CNVCentricBuilder(
            old_config, sql_context, consequence_builder, observation_builder
        ),
        builders.CNVOccurrenceCentricBuilder(
            old_config, sql_context, consequence_builder, observation_builder
        ),
        builders.GeneCentricBuilder(
            old_config, sql_context, consequence_builder, observation_builder
        ),
        builders.SSMCentricBuilder(
            old_config, sql_context, consequence_builder, observation_builder
        ),
        builders.SSMOccurrenceCentricBuilder(
            old_config, sql_context, consequence_builder, observation_builder
        ),
    )
    index_builders = map(BaseBuilderAdapter, old_builders)

    return tuple(b for b in index_builders if b.index in indices)


def _remove_unused_builders(
    input_builders: Iterable[builders.Builder],
    index_builders: Iterable[builders.Builder],
) -> Iterable[builders.Builder]:
    """Remove all builders not required directly or indirectly by the index builders.

    Args:
        input_builders: All input builders associated with the viz driver.
        index_builders: The index builders which were configured to run as part of this
            build.

    Returns:
        All index builders as well as any input builders required to run their build
        functionality. The builders are returned in topological order.
    """
    graph = nx.DiGraph()
    builders = {b.output: b for b in itertools.chain(input_builders, index_builders)}
    required_outputs = {b.output for b in index_builders}

    for builder in builders.values():
        graph.add_edges_from((i, builder.output) for i in builder.inputs)

    for builder in index_builders:
        required_outputs.update(nx.bfs_tree(graph, builder.output, reverse=True).nodes)

    return (builders[o] for o in nx.topological_sort(graph) if o in required_outputs)


class Driver(driver.Driver):
    @classmethod
    @contextlib.contextmanager
    def _load_dependencies(
        cls, config: configuration.Configuration
    ) -> Iterator[Dependencies]:
        """Loads all dependencies required by builders associated with this driver.

        Args:
            config: The configuration for this given run of the driver.

        Returns:
            A context manager in which all dependencies for the builders can be found.
        """
        index_client = cls.load_index_client(config.indexd)
        mappings_loader = es_utils.MappingsLoader()
        schema_loader = es_utils.SchemaLoader()

        with cls.load_es_client(
            config.elasticsearch.connection
        ) as es_client, cls.load_spark_session() as spark_session:
            yield Dependencies(
                spark_session=spark_session,
                es_client=es_client,
                es_dataframe_util=es_utils.DataFrameUtil(
                    config.elasticsearch,
                    spark_session,
                    es_client,
                    mappings_loader,
                    schema_loader,
                ),
                es_rdd_util=es_utils.RDDUtil(
                    config.elasticsearch, spark_session.sparkContext
                ),
                doc_dataframe_util=indexd_utils.DataFrameUtil(
                    index_client, spark_session
                ),
                case_field_selector=es_utils.CaseFieldSelector(),
                consequence_builder=builders.ConsequenceBuilder(),
                observation_builder=builders.ObservationBuilder(),
            )

    @classmethod
    @contextlib.contextmanager
    def _load_builders(
        cls, config: configuration.Configuration
    ) -> Iterator[Iterable[builders.Builder]]:
        with cls._load_dependencies(config) as dependencies:
            input_builders = _load_input_builders(config, dependencies)
            index_builders = _load_index_builders(config, dependencies)

            yield _remove_unused_builders(input_builders, index_builders)
