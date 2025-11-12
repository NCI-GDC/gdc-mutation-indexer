"""This module is for implementing how the viz driver will be executed."""

import contextlib
import logging
import pathlib
from collections.abc import Container, Iterable, Iterator
from typing import NamedTuple

import elasticsearch
from pyspark import sql

from mutation_indexer import driver, es_utils, indexd_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import builders, configuration
from mutation_indexer.viz.builders import civic, maf_metadata


class Dependencies(NamedTuple):
    """A class containing the dependencies needed for the viz driver."""

    case_field_selector: es_utils.CaseFieldSelector
    consequence_builder: builders.ConsequenceBuilder
    doc_dataframe_util: indexd_utils.DataFrameUtil
    es_client: elasticsearch.Elasticsearch
    es_dataframe_util: es_utils.DataFrameUtil
    es_rdd_util: es_utils.RDDUtil
    maf_file_filter_factory: maf_metadata.MAFFileFilterFactory
    mappings_loader: es_utils.MappingsLoader
    observation_builder: builders.ObservationBuilder
    spark_session: sql.SparkSession


class Driver(driver.Driver[configuration.Configuration]):
    @classmethod
    def load_config(cls, file: pathlib.Path) -> configuration.Configuration:
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
        with es_utils.initialize_client(config.elasticsearch) as es_client:
            index_client = driver.get_index_client(config.indexd)
            mappings_loader = es_utils.MappingsLoader()

            yield Dependencies(
                es_utils.CaseFieldSelector(),
                builders.ConsequenceBuilder(),
                indexd_utils.DataFrameUtil(
                    index_client,
                    spark_session,
                    logging.getLogger(indexd_utils.__name__),
                ),
                es_client,
                es_utils.DataFrameUtil(
                    config.elasticsearch,
                    spark_session,
                    es_client,
                    mappings_loader,
                    es_utils.SchemaLoader(),
                ),
                es_utils.RDDUtil(config.elasticsearch, spark_session.sparkContext),
                maf_metadata.MAFFileFilterFactory(config.elasticsearch.read, es_client),
                mappings_loader,
                builders.ObservationBuilder(),
                spark_session,
            )

    def _input_builders(
        self, config: configuration.Builders, dependencies: Dependencies
    ) -> Iterable[builders.Builder]:
        """The input builders associated with the viz driver & used by other builders.

        Args:
            config: The configuration for builders in this run of the driver.
            dependencies: The dependencies for the builders.

        Return:
            An iterable of all viz input builders.
        """
        return (
            builders.ASCATMetadataBuilder(
                config.ascat_metadata,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.es_rdd_util,
            ),
            builders.ASCATBuilder(
                config.ascat,
                dependencies.spark_session,
                dependencies.doc_dataframe_util,
            ),
            builders.CaseBuilder(
                config.case,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.case_field_selector,
            ),
            civic.DNABuilder(config.civic_dna, dependencies.spark_session),
            civic.ProteinBuilder(config.civic_protein, dependencies.spark_session),
            builders.GeneModelBuilder(config.gene_model, dependencies.spark_session),
            builders.MAFBuilder(
                config.maf, dependencies.spark_session, dependencies.doc_dataframe_util
            ),
            builders.MAFMetadataBuilder(
                config.maf_metadata,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.maf_file_filter_factory,
            ),
            builders.PrimaryAliquotBuilder(
                config.primary_aliquot,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.es_rdd_util,
            ),
            builders.SegmentCNVBuilder(
                config.segment_cnv,
                dependencies.spark_session,
                dependencies.doc_dataframe_util,
            ),
            builders.SegmentCNVMetadataBuilder(
                config.segment_cnv_metadata,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
            ),
        )

    def _index_builders(
        self,
        config: configuration.Builders,
        index_types: Container[build.IndexType],
        dependencies: Dependencies,
    ) -> Iterator[builders.Builder]:
        """Initializes all required index builders.

        Args:
            config: The configuration for the builders in this run.
            index_types: The required index types for this run of the driver.
            dependencies: All dependencies which the builders may require.

        Yields:
            The index builders required for this run.
        """
        if build.IndexType.CASE_CENTRIC in index_types:
            yield builders.CaseCentricBuilder(
                config.case_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.case_field_selector,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

        if build.IndexType.CNV_CENTRIC in index_types:
            yield builders.CNVCentricBuilder(
                config.cnv_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

        if build.IndexType.CNV_OCCURRENCE_CENTRIC in index_types:
            yield builders.CNVOccurrenceCentricBuilder(
                config.cnv_occurrence_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

        if build.IndexType.GENE_CENTRIC in index_types:
            yield builders.GeneCentricBuilder(
                config.gene_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

        if build.IndexType.SEGMENT_CNV_CENTRIC in index_types:
            yield builders.SegmentCNVCentricBuilder(
                config.segment_cnv_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
            )

        if build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC in index_types:
            yield builders.SegmentCNVOccurrenceCentricBuilder(
                config.segment_cnv_occurrence_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
            )

        if build.IndexType.SSM_CENTRIC in index_types:
            yield builders.SSMCentricBuilder(
                config.ssm_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

        if build.IndexType.SSM_OCCURRENCE_CENTRIC in index_types:
            yield builders.SSMOccurrenceCentricBuilder(
                config.ssm_occurrence_centric,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
                dependencies.consequence_builder,
                dependencies.observation_builder,
            )

    @contextlib.contextmanager
    def _initialize_builders(
        self, config: configuration.Configuration, spark_session: sql.SparkSession
    ) -> Iterator[Iterable[builders.Builder]]:
        with self._initialize_dependencies(config, spark_session) as dependencies:
            input_builders = self._input_builders(config.builders, dependencies)
            index_builders = self._index_builders(
                config.builders, config.build.index_types, dependencies
            )

            yield (*input_builders, *index_builders)


def main() -> None:
    """The main functionality for this driver module."""
    driver.main(Driver())
