import logging

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

        self.maf_only_indices = {'ssm_centric', 'ssm_occurrence_centric'}
        self.gistic_only_indices = {'cnv_centric', 'cnv_occurrence_centric'}

    def need_to_build_maf(self):
        """Return whether the MAF df is needed given the current config."""
        return len(set(self.config.index_types) - self.gistic_only_indices) > 0

    def need_to_build_gistic(self):
        """Return whether the Gistic df is needed given the current config."""
        return len(set(self.config.index_types) - self.maf_only_indices) > 0

    def run_export(self):
        # Combine MAFs into one DataFrame
        if self.need_to_build_maf():
            self.sc.setJobGroup('MAFBuilder', 'Build MAF dataframe')
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        else:
            self.logger.warn('Skipping MAF dataframe')
            maf_df = None

        # Combine Gistics into one DataFrame
        if self.need_to_build_gistic():
            self.sc.setJobGroup('GisticBuilder', 'Build Gistic dataframe')
            gistic_df = GisticBuilder(self.config, self.sqlContext).build()
        else:
            self.logger.warn('Skipping Gistic dataframe')
            gistic_df = None

        # Use maf_df and gistic_df to build case DataFrame
        self.sc.setJobGroup('CaseBuilder', 'Build Case dataframe')
        case_df = CaseBuilder(self.config,
                              self.sqlContext).build(maf_df, gistic_df)

        for builder in self.builders:
            index_name = builder.index_name
            if index_name in self.config.index_types:
                self.sc.setJobGroup(index_name, 'Build {}'.format(index_name))
                if index_name in self.maf_only_indices:
                    # these builders do not yet depend on gistic_df
                    builder(self.config, self.sqlContext).build(maf_df, case_df).load()
                elif index_name in self.gistic_only_indices:
                    # these builders do not depend on maf_df
                    builder(self.config, self.sqlContext).build(gistic_df, case_df).load()
                else:
                    builder(self.config,
                            self.sqlContext).build(maf_df, gistic_df, case_df).load()

        self.logger.info('Mutation Indexer finished successfully')
