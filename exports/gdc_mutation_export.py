import logging

import pyspark
from config import LOG_FORMAT
from exports import builders
from pyspark import sql

logging.basicConfig(format=LOG_FORMAT)


class GDCMutationExport:
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, spark_session: sql.SparkSession, config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._spark_session = spark_session
        self._spark_context = spark_session.sparkContext  # type: pyspark.SparkContext
        self.builders = [
            builders.CaseCentricBuilder,
            builders.GeneCentricBuilder,
            builders.GeneExpressionBuilder,
            builders.SSMCentricBuilder,
            builders.SSMOccurrenceCentricBuilder,
            builders.CNVCentricBuilder,
            builders.CNVOccurrenceCentricBuilder,
        ]

    def build_input_data_frames(self):

        # Combine MAFs into one DataFrame
        self._spark_context.setJobGroup("MAFBuilder", "Build MAF dataframe")
        maf_df = builders.MAFBuilder(self.config, self._spark_session).build()

        # Combine Gistics into one DataFrame
        self._spark_context.setJobGroup("GisticBuilder", "Build Gistic dataframe")
        gistic_df = builders.GisticBuilder(self.config, self._spark_session).build()

        # Use maf_df and gistic_df to build case DataFrame
        self._spark_context.setJobGroup("CaseBuilder", "Build Case dataframe")
        case_df = builders.CaseBuilder(self.config, self._spark_session).build(maf_df, gistic_df)
        sub_case_df = case_df.drop("summary")
        sub_case_df.persist()

        return maf_df, gistic_df, case_df, sub_case_df

    def run_export(self):
        maf_df = None
        gistic_df = None
        case_df = None
        sub_case_df = None

        for builder in self.builders:
            index_name = builder.index_name

            if index_name not in self.config.index_types:
                continue

            self._spark_context.setJobGroup(index_name, "Build {}".format(index_name))

            primary_aliquot_builder = builders.PrimaryAliquotBuilder(
                self.config, self._spark_session
            )
            consequence_builder = builders.ConsequenceBuilder(self.config, self._spark_session)
            observation_builder = builders.ObservationBuilder(primary_aliquot_builder)

            active_builder = builder(
                self.config,
                self._spark_session,
                consequence_builder=consequence_builder,
                observation_builder=observation_builder,
            )

            if index_name == "gene_expression":
                self._spark_context.setJobGroup(
                    "GeneExpressionCaseInputBuilder", "Build GE CaseInput df"
                )
                ge_case_df = builders.GeneExpressionCaseInputBuilder(
                    self.config,
                    self._spark_session,
                    "gene_expression_cases",
                ).build()

                self._spark_context.setJobGroup(
                    "GeneExpressionValueInputBuilder", "Build GE ValueInput df"
                )
                ge_values_df = builders.GeneExpressionValueInputBuilder(
                    self.config,
                    self._spark_session,
                    "gene_expression_values",
                ).build()
                active_builder.build(ge_case_df, ge_values_df).load()
                continue

            if maf_df is None:
                maf_df, gistic_df, case_df, sub_case_df = self.build_input_data_frames()

            if index_name == "case_centric":
                active_builder.build(maf_df, gistic_df, case_df).load()
            elif index_name in ["ssm_centric", "ssm_occurrence_centric"]:
                # these builders do not yet depend on gistic_df
                active_builder.build(maf_df, sub_case_df).load()
            elif index_name in ["cnv_centric", "cnv_occurrence_centric"]:
                # these builders do not depend on maf_df
                active_builder.build(gistic_df, sub_case_df).load()
            else:
                active_builder.build(maf_df, gistic_df, sub_case_df).load()

        self.logger.info("Mutation Indexer finished successfully")
