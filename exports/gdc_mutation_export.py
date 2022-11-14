import abc
import logging
from typing import Collection, Generic, Mapping, NamedTuple, TypeVar

import pyspark
from pyspark import sql
from typing_extensions import TypedDict

import config
from exports.builders import base_builder, base_input_builder
from exports.constants import build

logging.basicConfig(format=config.LOG_FORMAT)

logger = logging.getLogger(__name__)


class Builders(NamedTuple):
    input_builders: Mapping[build.DataFrame, base_input_builder.BaseInputBuilder]
    index_builders: Mapping[build.IndexType, base_builder.BaseBuilder]


class VizInputs(TypedDict):
    gene_model_df: sql.DataFrame
    maf_metadata_df: sql.DataFrame
    maf_df: sql.DataFrame
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class GEInputs(TypedDict):
    case_df: sql.DataFrame
    value_df: sql.DataFrame


TInputs = TypeVar("TInputs", VizInputs, GEInputs)


class GDCMutationExport(Generic[TInputs], abc.ABC):
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

    @abc.abstractmethod
    def build_input_data_frames(self) -> TInputs:
        pass

    def run_export(
        self,
    ) -> None:
        inputs = self.build_input_data_frames()

        for index_type in self._index_types:
            if index_type not in self._index_builders:
                raise NotImplementedError(
                    f"No builder is configured for index: {index_type}"
                )

            self._spark_context.setJobGroup(index_type.name, f"Build {index_type}")
            self._index_builders[index_type].build(**inputs).load()

        logger.info("Mutation Indexer finished successfully")


class VizExport(GDCMutationExport[VizInputs]):
    def __init__(
        self,
        sc: pyspark.SparkContext,
        index_types: Collection[build.IndexType],
        builders: Builders,
    ) -> None:
        super().__init__(sc, index_types, builders)

    def build_input_data_frames(self) -> VizInputs:
        # Load gene model
        self._spark_context.setJobGroup(
            "GeneModelBuilder", "Build Gene Model Dataframe"
        )
        gene_model_df = self._input_builders[build.DataFrame.GENE_MODEL].build()

        # Load primary aliquot data
        self._spark_context.setJobGroup(
            "PrimaryAliquotBuilder", "Build Primary Aliquot Dataframe"
        )
        primary_aliquot_df = self._input_builders[
            build.DataFrame.PRIMARY_ALIQUOT
        ].build()

        self._spark_context.setJobGroup(
            "MAFMetadataBuilder", "Build MAF Metadata Dataframe"
        )
        maf_metadata_df = self._input_builders[build.DataFrame.MAF_METADATA].build()

        # Combine MAFs into one DataFrame
        self._spark_context.setJobGroup("MAFBuilder", "Build MAF dataframe")
        maf_df = self._input_builders[build.DataFrame.MAF].build(
            maf_metadata_df=maf_metadata_df, gene_model_df=gene_model_df
        )

        # Create dataframe from ASCAT data
        self._spark_context.setJobGroup("AscatBuilder", "Build Ascat dataframe")
        ascat_df = self._input_builders[build.DataFrame.ASCAT].build(
            primary_aliquot_df=primary_aliquot_df, gene_model_df=gene_model_df
        )

        # Use maf_df and ascat_df to build case DataFrame
        self._spark_context.setJobGroup("CaseBuilder", "Build Case dataframe")
        case_df = self._input_builders[build.DataFrame.CASE].build(
            maf_metadata_df=maf_metadata_df, ascat_df=ascat_df
        )

        return VizInputs(
            gene_model_df=gene_model_df,
            maf_metadata_df=maf_metadata_df,
            maf_df=maf_df,
            ascat_df=ascat_df,
            case_df=case_df,
            primary_aliquot_df=primary_aliquot_df,
        )


class GEExport(GDCMutationExport[GEInputs]):
    def __init__(
        self,
        sc: pyspark.SparkContext,
        index_types: Collection[build.IndexType],
        builders: Builders,
    ) -> None:
        super().__init__(sc, index_types, builders)

    def build_input_data_frames(self) -> GEInputs:
        self._spark_context.setJobGroup("GeneModelBuilder", "Build Gene Model df")
        gene_model_df = self._input_builders[build.DataFrame.GENE_MODEL].build()

        self._spark_context.setJobGroup(
            "GeneExpressionPrimaryAliquotBuilder", "Build GE Primary Aliquot df"
        )
        primary_aliquot_df = self._input_builders[
            build.DataFrame.PRIMARY_ALIQUOT
        ].build()

        self._spark_context.setJobGroup(
            "GeneExpressionCaseInputBuilder", "Build GE CaseInput df"
        )
        case_df = self._input_builders[build.DataFrame.CASE].build(
            gene_expression_primary_aliquot_df=primary_aliquot_df
        )

        self._spark_context.setJobGroup(
            "GeneExpressionValueInputBuilder", "Build GE ValueInput df"
        )
        value_df = self._input_builders[build.DataFrame.EXPRESION_VALUE].build(
            gene_model_df=gene_model_df,
            gene_expression_primary_aliquot_df=primary_aliquot_df,
        )

        return GEInputs(case_df=case_df, value_df=value_df)
