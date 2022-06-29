import logging
from typing import Iterable, NamedTuple

import pyspark
from pyspark import sql

import config
from exports import builders, es_utils, indexd_utils
from exports.builders import ascat
from exports.builders.clinical_annotations import civic

logging.basicConfig(format=config.LOG_FORMAT)

logger = logging.getLogger("exports")


class BuilderInputs(NamedTuple):
    gene_model_df: sql.DataFrame
    maf_df: sql.DataFrame
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame
    sub_case_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class GDCMutationExport:
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(
        self,
        sc: pyspark.SparkContext,
        sqlContext: sql.SQLContext,
        config: config.BaseConfig,
    ) -> None:
        self.config = config
        self.logger = logger
        self.sc = sc
        self.sqlContext = sqlContext

    def build_input_data_frames(self) -> BuilderInputs:
        doc_dataframe_util = indexd_utils.DataFrameUtil(
            self.config.indexd, self.sqlContext, self.logger
        )
        es_dataframe_util = es_utils.DataFrameUtil(self.config, self.sqlContext)
        es_rdd_util = es_utils.RDDUtil(self.config, self.sc)

        # Load gene model
        self.sc.setJobGroup("GeneModelBuilder", "Build Gene Model Dataframe")
        gene_model_df = builders.GeneModelBuilder(self.config, self.sqlContext).build()

        # Load primary aliquot data
        self.sc.setJobGroup("PrimaryAliquotBuilder", "Build Primary Aliquot Dataframe")
        primary_aliquot_df = builders.PrimaryAliquotBuilder(
            self.config,
            self.sqlContext,
            self.config.indexd,
            es_dataframe_util,
            es_rdd_util,
        ).build()

        self.sc.setJobGroup("MAFMetadataBuilder", "Build MAF Metadata Dataframe")
        maf_metadata_df = builders.MAFMetadataBuilder(
            self.config,
            self.sqlContext,
            es_dataframe_util,
        ).build()

        # Combine MAFs into one DataFrame
        self.sc.setJobGroup("MAFBuilder", "Build MAF dataframe")
        annotation_builders = (civic.CivicBuilder(self.config, self.sqlContext),)
        maf_df = builders.MAFBuilder(
            self.config, self.sqlContext, doc_dataframe_util, annotation_builders
        ).build(maf_metadata_df=maf_metadata_df, gene_model_df=gene_model_df)

        # Create dataframe from ASCAT data
        self.sc.setJobGroup("AscatBuilder", "Build Ascat dataframe")
        ascat_df = (
            ascat.load_empty_ascat_data(self.sqlContext)
            if self.config.omit_cnv_data
            else builders.AscatBuilder(
                self.config,
                self.sqlContext,
                doc_dataframe_util,
                es_dataframe_util,
                self.config.es,
            ).build(primary_aliquot_df=primary_aliquot_df, gene_model_df=gene_model_df)
        )

        # Use maf_df and ascat_df to build case DataFrame
        self.sc.setJobGroup("CaseBuilder", "Build Case dataframe")
        case_df = builders.CaseBuilder(
            self.config, self.sqlContext, es_dataframe_util
        ).build(maf_metadata_df=maf_metadata_df, ascat_df=ascat_df)
        sub_case_df = case_df.drop("summary")
        sub_case_df.persist()

        return BuilderInputs(
            gene_model_df, maf_df, ascat_df, case_df, sub_case_df, primary_aliquot_df
        )

    def run_gene_expression_export(self) -> None:
        """
        Runs the importing, building, and uploading of the gene expression data into elasticsearch.
        """
        es_dataframe_util = es_utils.DataFrameUtil(self.config, self.sqlContext)
        doc_dataframe_util = indexd_utils.DataFrameUtil(
            self.config.indexd, self.sqlContext, logger
        )

        self.sc.setJobGroup("GeneModelBuilder", "Build Gene Model df")
        gene_model_df = builders.GeneModelBuilder(self.config, self.sqlContext).build()

        self.sc.setJobGroup(
            "GeneExpressionPrimaryAliquotBuilder", "Build GE Primary Aliquot df"
        )
        primary_aliquot_df = builders.GeneExpressionPrimaryAliquotBuilder(
            self.config, self.sqlContext, es_dataframe_util
        ).build()

        self.sc.setJobGroup("GeneExpressionCaseInputBuilder", "Build GE CaseInput df")
        ge_case_df = builders.GeneExpressionCaseInputBuilder(
            self.config, self.sqlContext
        ).build(gene_expression_primary_aliquot_df=primary_aliquot_df)

        self.sc.setJobGroup("GeneExpressionValueInputBuilder", "Build GE ValueInput df")
        ge_values_df = builders.GeneExpressionValueInputBuilder(
            self.config, self.sqlContext, doc_dataframe_util
        ).build(
            gene_model_df=gene_model_df,
            gene_expression_primary_aliquot_df=primary_aliquot_df,
        )

        self.sc.setJobGroup("gene_expression", "Build {}".format("gene_expression"))
        builders.GeneExpressionBuilder(self.config, self.sqlContext).build(
            ge_case_df, ge_values_df
        ).load()

    def run_core_exports(self, index_names: Iterable[str]) -> None:
        inputs = self.build_input_data_frames()
        consequence_builder = builders.ConsequenceBuilder(self.config, self.sqlContext)
        observation_builder = builders.ObservationBuilder()

        for index_name in index_names:
            self.sc.setJobGroup(index_name, "Build {}".format(index_name))

            if index_name == "case_centric":
                builders.CaseCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(
                    inputs.maf_df,
                    inputs.ascat_df,
                    inputs.case_df,
                    inputs.primary_aliquot_df,
                ).load()

            elif index_name == "ssm_centric":
                builders.SSMCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(
                    inputs.maf_df, inputs.sub_case_df, inputs.primary_aliquot_df
                ).load()

            elif index_name == "ssm_occurrence_centric":
                builders.SSMOccurrenceCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(
                    inputs.maf_df, inputs.sub_case_df, inputs.primary_aliquot_df
                ).load()

            elif index_name == "cnv_centric":
                builders.CNVCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(inputs.ascat_df, inputs.sub_case_df).load()

            elif index_name == "cnv_occurrence_centric":
                builders.CNVOccurrenceCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(inputs.ascat_df, inputs.sub_case_df).load()

            elif index_name == "gene_centric":
                builders.GeneCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                ).build(
                    inputs.maf_df,
                    inputs.ascat_df,
                    inputs.sub_case_df,
                    inputs.primary_aliquot_df,
                ).load()

            else:
                raise NotImplementedError(
                    "No builder is configured for index: {}".format(index_name)
                )

    def run_export(self) -> None:
        index_names = self.config.index_types  # type: Iterable[str]

        if any(index_name == "gene_expression" for index_name in index_names):
            self.run_gene_expression_export()

        self.run_core_exports(
            filter(lambda index_name: index_name != "gene_expression", index_names)
        )

        self.logger.info("Mutation Indexer finished successfully")
