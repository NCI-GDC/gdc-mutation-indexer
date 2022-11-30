import abc
import logging
from typing import Collection, Dict, Iterable, Mapping, NamedTuple

import pyspark
from pyspark import sql

import config
from exports.builders import base_builder, base_input_builder
from exports.constants import build

logging.basicConfig(format=config.LOG_FORMAT)

logger = logging.getLogger(__name__)


class Builders(NamedTuple):
    input_builders: Mapping[build.DataFrame, base_input_builder.BaseInputBuilder]
    index_builders: Mapping[build.IndexType, base_builder.BaseBuilder]


class GDCMutationExport(abc.ABC):
    """
    The main entry point into the index export process for the mutation indices
    """

    __slots__ = ("_spark_context", "_index_types", "_input_builders", "_index_builders")

    def __init__(
        self,
        spark_context: pyspark.SparkContext,
        index_types: Collection[build.IndexType],
        builders: Builders,
    ) -> None:
        self._spark_context = spark_context
        self._index_types = index_types
        self._input_builders = builders.input_builders
        self._index_builders = builders.index_builders

    @property
    @abc.abstractmethod
    def _input_data_frames(self) -> Iterable[build.DataFrame]:
        """
        An ordered iteration of the required data frames for the export process.
        """
        pass

    def _build_input_data_frames(self) -> Dict[str, sql.DataFrame]:
        """
        Builds the input data frames given by the input data frames prop using their
        related input builder.

        Returns:
            All data frames output by the input data frame builders.
        """
        inputs: Dict[str, sql.DataFrame] = {}

        for data_frame in self._input_data_frames:
            if data_frame not in self._input_builders:
                raise ValueError(
                    f"No builder is configured for DataFrame: {data_frame}."
                )

            self._spark_context.setJobGroup(data_frame.name, f"Build {data_frame}")

            df = self._input_builders[data_frame].build(**inputs)
            inputs[f"{data_frame.name.lower()}_df"] = df

        return inputs

    def run_export(
        self,
    ) -> None:
        """
        Executes the export configured data by running the required builders.
        """
        inputs = self._build_input_data_frames()

        for index_type in self._index_types:
            if index_type not in self._index_builders:
                raise ValueError(f"No builder is configured for index: {index_type}.")

            self._spark_context.setJobGroup(index_type.name, f"Build {index_type}")
            self._index_builders[index_type].build(**inputs).load()

        logger.info("Mutation Indexer finished successfully")


class VizExport(GDCMutationExport):
    def __init__(
        self,
        sc: pyspark.SparkContext,
        index_types: Collection[build.IndexType],
        builders: Builders,
    ) -> None:
        super().__init__(sc, index_types, builders)

    @property
    def _input_data_frames(self) -> Iterable[build.DataFrame]:
        return (
            build.DataFrame.GENE_MODEL,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.MAF_METADATA,
            build.DataFrame.MAF,
            build.DataFrame.ASCAT,
            build.DataFrame.CASE,
        )


class GEExport(GDCMutationExport):
    def __init__(
        self,
        sc: pyspark.SparkContext,
        index_types: Collection[build.IndexType],
        builders: Builders,
    ) -> None:
        super().__init__(sc, index_types, builders)

    @property
    def _input_data_frames(self) -> Iterable[build.DataFrame]:
        return (
            build.DataFrame.GENE_MODEL,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.CASE,
            build.DataFrame.EXPRESSION_VALUE,
        )
