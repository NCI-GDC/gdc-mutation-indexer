from config import BaseConfig
from builders import MAFBuilder, CaseCentricBuilder, GeneCentricBuilder


class GDCMutationExport(object):
    '''
    The main entry point into the index export process for the mutation indices
    '''

    def __init__(self, sc, sqlContext, config=BaseConfig()):
        self.config = config
        self.sc = sc
        self.sqlContext = sqlContext

    
    def run_export(self, config=None):
        # Construct master MAF from all individual MAF
        df = MAFBuilder(self.config, self.sqlContext).build()

        #CaseCentricBuilder(self.config, self.sqlContext).build(df).load()
        GeneCentricBuilder(self.config, self.sqlContext).build(df).load()

