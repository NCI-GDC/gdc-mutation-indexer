import logging
from typing import Iterable, NamedTuple

import config
from exports import builders, es_utils
from pyspark import sql

logging.basicConfig(format=config.LOG_FORMAT)


BuilderInputs = NamedTuple(
    "BuilderInputs",
    [
        ("gene_model_df", sql.DataFrame),
        ("maf_df", sql.DataFrame),
        ("gistic_df", sql.DataFrame),
        ("case_df", sql.DataFrame),
        ("sub_case_df", sql.DataFrame),
        ("primary_aliquot_df", sql.DataFrame),
    ],
)


class GDCMutationExport(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, sc, sqlContext: sql.SQLContext, config: config.BaseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sc = sc
        self.sqlContext = sqlContext

    def build_input_data_frames(self) -> BuilderInputs:
        es_dataframe_util = es_utils.DataFrameUtil(self.config, self.sqlContext)

        # Load gene model
        gene_model_df = builders.GeneModelBuilder(self.config, self.sqlContext).build()

        # Load primary aliquot data
        self.sc.setJobGroup("PrimaryAliquotBuilder", "Build Primary Aliquot Dataframe")
        primary_aliquot_df = builders.PrimaryAliquotBuilder(
            self.config, self.sqlContext, self.config.indexd, es_dataframe_util
        ).build()

        # Combine MAFs into one DataFrame
        self.sc.setJobGroup("MAFBuilder", "Build MAF dataframe")
        maf_df = builders.MAFBuilder(self.config, self.sqlContext).build(gene_model_df=gene_model_df)

        # Combine Gistics into one DataFrame
        self.sc.setJobGroup("GisticBuilder", "Build Gistic dataframe")
        gistic_df = builders.GisticBuilder(self.config, self.sqlContext).build(gene_model_df=gene_model_df)

        # Use maf_df and gistic_df to build case DataFrame
        self.sc.setJobGroup("CaseBuilder", "Build Case dataframe")
        case_df = builders.CaseBuilder(self.config, self.sqlContext).build(
            maf_df, gistic_df
        )
        sub_case_df = case_df.drop("summary")
        sub_case_df.persist()

        return BuilderInputs(
            gene_model_df, maf_df, gistic_df, case_df, sub_case_df, primary_aliquot_df
        )

    def run_gene_expression_export(self):
        es_dataframe_util = es_utils.DataFrameUtil(self.config, self.sqlContext)
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            self.config, self.sqlContext, self.config.indexd, es_dataframe_util
        )
        active_builder = builders.GeneExpressionBuilder(self.config, self.sqlContext)

        self.sc.setJobGroup("GeneExpressionCaseInputBuilder", "Build GE CaseInput df")
        ge_case_df = builders.GeneExpressionCaseInputBuilder(
            self.config,
            self.sqlContext,
            primary_aliquot_builder,
        ).build()

        self.sc.setJobGroup("GeneExpressionValueInputBuilder", "Build GE ValueInput df")
        ge_values_df = builders.GeneExpressionValueInputBuilder(
            self.config,
            self.sqlContext,
            primary_aliquot_builder,
        ).build()

        self.sc.setJobGroup("gene_expression", "Build {}".format("gene_expression"))
        active_builder.build(ge_case_df, ge_values_df).load()

    def run_core_exports(self, index_names: Iterable[str]):
        (
            gene_model_df,
            maf_df,
            gistic_df,
            case_df,
            sub_case_df,
            primary_aliquot_df,
        ) = self.build_input_data_frames()
        consequence_builder = builders.ConsequenceBuilder(self.config, self.sqlContext)
        observation_builder = builders.ObservationBuilder()

        for index_name in index_names:
            self.sc.setJobGroup(index_name, "Build {}".format(index_name))

            if index_name == "case_centric":
                active_builder = builders.CaseCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(
                    maf_df, gistic_df, case_df, primary_aliquot_df
                ).load()
            elif index_name == "ssm_centric":
                active_builder = builders.SSMCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(maf_df, sub_case_df, primary_aliquot_df).load()
            elif index_name == "ssm_occurrence_centric":
                active_builder = builders.SSMOccurrenceCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(maf_df, sub_case_df, primary_aliquot_df).load()
            elif index_name == "cnv_centric":
                active_builder = builders.CNVCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(gistic_df, sub_case_df).load()
            elif index_name == "cnv_occurrence_centric":
                active_builder = builders.CNVOccurrenceCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(gistic_df, sub_case_df).load()
            elif index_name == "gene_centric":
                active_builder = builders.GeneCentricBuilder(
                    self.config,
                    self.sqlContext,
                    consequence_builder,
                    observation_builder,
                )

                active_builder.build(
                    maf_df, gistic_df, sub_case_df, primary_aliquot_df
                ).load()

            else:
                raise NotImplementedError(
                    "No builder is configured for index: {}".format(index_name)
                )

    def run_export(self):
        index_names = self.config.index_types  # type: Iterable[str]

        if any(index_name == "gene_expression" for index_name in index_names):
            self.run_gene_expression_export()

        self.run_core_exports(
            filter(lambda index_name: index_name != "gene_expression", index_names)
        )

        self.logger.info("Mutation Indexer finished successfully")
