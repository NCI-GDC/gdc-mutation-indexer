from config import BaseConfig
from builders import (
    MAFBuilder,
    CaseCentricBuilder,
    GeneCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)


class GDCMutationExport(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, sc, sqlContext, config=BaseConfig()):
        self.config = config
        self.sc = sc
        self.sqlContext = sqlContext

    def run_export(self, config=None):
        # Construct master MAF from all individual MAFs
        df = MAFBuilder(self.config, self.sqlContext).build()

        if ('case_centric' in config.index_names
                and config.index_names['case_centric'] is not None):
            CaseCentricBuilder(self.config, self.sqlContext).build(df).load()

        if ('gene_centric' in config.index_names
                and config.index_names['gene_centric'] is not None):
            GeneCentricBuilder(self.config, self.sqlContext).build(df).load()

        if ('ssm_centric' in config.index_names
                and config.index_names['ssm_centric'] is not None):
            SSMCentricBuilder(self.config, self.sqlContext).build(df).load()

        if ('ssm_occurrence_centric' in config.index_names
                and config.index_names['ssm_occurrence_centric'] is not None):
            SSMOccurrenceCentricBuilder(self.config, self.sqlContext).build(df).load()
