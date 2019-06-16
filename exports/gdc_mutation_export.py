import logging

from parsers import (
    BuildArgs,
    S3Args,
    ESArgs,
)
from config import LOG_FORMAT
from builders import (
    MAFBuilder,
    GisticBuilder,
    CaseBuilder,
    CaseCentricBuilder,
    GeneCentricBuilder,
    ScoreCentricBuilder,
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
            ScoreCentricBuilder,
            SSMCentricBuilder,
            SSMOccurrenceCentricBuilder,
            CNVCentricBuilder,
            CNVOccurrenceCentricBuilder,
        ]

    def run_export(self):
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

        for builder in self.builders:
            index_name = builder.index_name
            if index_name in self.config.index_types:
                self.sc.setJobGroup(index_name, 'Build {}'.format(index_name))
                if index_name in ['ssm_centric', 'ssm_occurrence_centric']:
                    # these builders do not yet depend on gistic_df
                    builder(self.config, self.sqlContext).build(maf_df, case_df).load()
                elif index_name in ['cnv_centric', 'cnv_occurrence_centric']:
                    # these builders do not depend on maf_df
                    builder(self.config, self.sqlContext).build(gistic_df, case_df).load()
                else:
                    builder(self.config,
                            self.sqlContext).build(maf_df, gistic_df, case_df).load()

        self.logger.info('Mutation Indexer finished successfully')
