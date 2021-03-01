import logging

from config import LOG_FORMAT
from exports.builders import (
    MAFBuilder,
    GisticBuilder,
    CaseBuilder,
    CaseCentricBuilder,
    ConsequenceBuilder,
    GeneCentricBuilder,
    GeneExpressionBuilder,
    GeneExpressionCaseInputBuilder,
    GeneExpressionValueInputBuilder,
    ObservationBuilder,
    PrimaryAliquotBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder,
    CNVCentricBuilder,
    CNVOccurrenceCentricBuilder,
)

logging.basicConfig(format=LOG_FORMAT)


class GDCMutationExport(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, sc, sqlContext, config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sc = sc
        self.sqlContext = sqlContext
        self.builders = [
            CaseCentricBuilder,
            GeneCentricBuilder,
            GeneExpressionBuilder,
            SSMCentricBuilder,
            SSMOccurrenceCentricBuilder,
            CNVCentricBuilder,
            CNVOccurrenceCentricBuilder,
        ]

    def build_input_data_frames(self):

        # Combine MAFs into one DataFrame
        self.sc.setJobGroup('MAFBuilder', 'Build MAF dataframe')
        maf_df = MAFBuilder(self.config, self.sqlContext).build()

        # Combine Gistics into one DataFrame
        self.sc.setJobGroup('GisticBuilder', 'Build Gistic dataframe')
        gistic_df = GisticBuilder(self.config, self.sqlContext).build()

        # Use maf_df and gistic_df to build case DataFrame
        self.sc.setJobGroup('CaseBuilder', 'Build Case dataframe')
        case_df = CaseBuilder(self.config,
                              self.sqlContext).build(maf_df, gistic_df)
        sub_case_df = case_df.drop('summary')
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

            self.sc.setJobGroup(index_name, 'Build {}'.format(index_name))

            if index_name == "gene_expression":
                self.sc.setJobGroup("GeneExpressionCaseInputBuilder", "Build GE CaseInput df")
                ge_case_df = GeneExpressionCaseInputBuilder(
                    self.config,
                    self.sqlContext,
                    "gene_expression_cases",
                ).build()

                self.sc.setJobGroup("GeneExpressionValueInputBuilder", "Build GE ValueInput df")
                ge_values_df = GeneExpressionValueInputBuilder(
                    self.config,
                    self.sqlContext,
                    "gene_expression_values",
                ).build()
                primary_aliquot_builder = PrimaryAliquotBuilder(self.config, self.sqlContext)

                builder(
                    self.config, 
                    self.sqlContext,
                    consequence_builder=ConsequenceBuilder(self.config, self.sqlContext),
                    observation_builder=ObservationBuilder(primary_aliquot_builder),
                ).build(ge_case_df, ge_values_df).load()

                continue

            if maf_df is None:
                maf_df, gistic_df, case_df, sub_case_df = self.build_input_data_frames()

            if index_name == 'case_centric':
                builder(self.config, self.sqlContext).build(maf_df, gistic_df, case_df).load()
            elif index_name in ['ssm_centric', 'ssm_occurrence_centric']:
                # these builders do not yet depend on gistic_df
                builder(self.config, self.sqlContext).build(maf_df, sub_case_df).load()
            elif index_name in ['cnv_centric', 'cnv_occurrence_centric']:
                # these builders do not depend on maf_df
                builder(self.config, self.sqlContext).build(gistic_df, sub_case_df).load()
            else:
                builder(self.config,
                        self.sqlContext).build(maf_df, gistic_df, sub_case_df).load()

        self.logger.info('Mutation Indexer finished successfully')
