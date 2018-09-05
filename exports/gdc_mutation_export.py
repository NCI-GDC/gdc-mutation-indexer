from builders import (
    MAFBuilder,
    GisticBuilder,
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
        self.build_map = {
            'case_centric': CaseCentricBuilder,
            'gene_centric': GeneCentricBuilder,
            'cnv_centric': CNVCentricBuilder,
            'ssm_centric': SSMCentricBuilder,
            'ssm_occurrence_centric': SSMOccurrenceCentricBuilder,
            'cnv_occurrence_centric': CNVOccurrenceCentricBuilder,
        }

    def run_export(self, config=None):
        # Construct master MAF from all individual MAFs
        maf_df = MAFBuilder(self.config, self.sqlContext).build()
        gistic_df = GisticBuilder(self.config, self.sqlContext).build()

        for index_name, builder in sorted(self.build_map.items(),
                                          key=lambda x: x[0]):
            if (index_name in config.index_names
                    and config.index_names[index_name] is not None):
                if index_name in ['ssm_centric', 'ssm_occurrence_centric']:
                    # these builders do not yet depend on gistic_df
                    builder(self.config, self.sqlContext).build(maf_df).load()
                else:
                    builder(self.config, self.sqlContext).build(maf_df, gistic_df).load()

