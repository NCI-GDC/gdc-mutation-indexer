from builders import (
    MAFBuilder,
    GisticBuilder,
    CaseBuilder,
    CaseCentricBuilder,
    GeneCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder,
    CNVCentricBuilder,
    CNVOccurrenceCentricBuilder,
)


class GDCMutationExport(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, sc, sqlContext, config):
        self.config = config
        self.sc = sc
        self.sqlContext = sqlContext
        self.builders = [
            CaseCentricBuilder,
            GeneCentricBuilder,
            SSMCentricBuilder,
            SSMOccurrenceCentricBuilder,
            CNVCentricBuilder,
            CNVOccurrenceCentricBuilder,
        ]

    def run_export(self, config=None):
        # Construct master MAF from all individual MAFs
        self.sc.setJobGroup('MAFBuilder', 'Build MAF dataframe')
        maf_df = MAFBuilder(self.config, self.sqlContext).build()

        self.sc.setJobGroup('GisticBuilder', 'Build Gistic dataframe')
        gistic_df = GisticBuilder(self.config, self.sqlContext).build()

        self.sc.setJobGroup('CaseBuilder', 'Build Case dataframe')
        case_df = CaseBuilder(self.config,
                              self.sqlContext).build(maf_df, gistic_df)

        for builder in self.builders:
            index_name = builder.index_name
            if index_name in config.index_names:
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
