import contextlib
import inspect
import itertools
from collections.abc import Iterable, Iterator
from typing import NamedTuple, cast

import elasticsearch
import networkx as nx
from pyspark import sql

from mutation_indexer import builders, configuration, driver, es_utils, indexd_utils
from mutation_indexer.builders import bases, civic, maf_metadata
from mutation_indexer.configuration import adapter
from mutation_indexer.constants import build


class BaseBuilderAdapter(bases.Builder):
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
            for p in inspect.signature(self.build).parameters.keys()
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
    config: configuration.Configuration, dependencies: Dependencies
) -> Iterable[bases.Builder]:
    viz = config.builders.viz
    spark_session = dependencies.spark_session
    es_dataframe_util = dependencies.es_dataframe_util
    es_rdd_util = dependencies.es_rdd_util
    doc_dataframe_util = dependencies.doc_dataframe_util
    case_field_selector = dependencies.case_field_selector
    file_filter_factory = maf_metadata.MAFFileFilterFactory(
        config.elasticsearch.read, dependencies.es_client
    )

    return (
        builders.ASCATMetadataBuilder(
            viz.ascat_metadata, spark_session, es_dataframe_util, es_rdd_util
        ),
        builders.ASCATBuilder(viz.ascat, spark_session, doc_dataframe_util),
        builders.CaseBuilder(
            viz.case, spark_session, es_dataframe_util, case_field_selector
        ),
        civic.DNABuilder(viz.civic_dna, spark_session),
        civic.ProteinBuilder(viz.civic_protein, spark_session),
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
    config: configuration.Configuration, dependencies: Dependencies
) -> Iterable[bases.Builder]:
    indices = config.build.index_types
    sql_context = cast(sql.SQLContext, dependencies.spark_session)
    old_config = adapter.ObsoleteConfig(config, dependencies.es_client)
    es_dataframe_util = dependencies.es_dataframe_util
    case_field_selector = dependencies.case_field_selector
    consequence_builder = dependencies.consequence_builder
    observation_builder = dependencies.observation_builder

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


class Driver(driver.Driver):
    @contextlib.contextmanager
    @classmethod
    def _load_dependencies(
        cls, config: configuration.Configuration
    ) -> Iterator[Dependencies]:
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
    def _load_builders(
        cls, config: configuration.Configuration
    ) -> Iterator[bases.Builder]:
        with cls._load_dependencies(config) as dependencies:
            input_builders = _load_input_builders(config, dependencies)
            index_builders = _load_index_builders(config, dependencies)
            graph = nx.Graph()

            for builder in itertools.chain(input_builders, index_builders):
                graph.add_node(builder.output, builder=builder)
                graph.add_edges_from((i, builder.output) for i in builder.inputs)

            reversed_graph = nx.reverse(graph)
            required_outputs = set()

            for builder in index_builders:
                required_outputs |= nx.descendants(reversed_graph, builder.output)

            yield from (
                graph.nodes.data()[n]["builder"]
                for n in nx.topological_sort(graph)
                if n in required_outputs
            )
