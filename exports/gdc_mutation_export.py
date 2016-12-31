from elasticsearch import Elasticsearch
from config import BaseConfig
from builders import MAFBuilder, CaseCentricBuilder


class GDCMutationExport(object):
    '''
    The main entry point into the index export process for the mutation indices
    '''

    def __init__(self, sc, sqlContext, config=BaseConfig):
        self.config = config
        self.sc = sc
        self.sqlContext = sqlContext

    
    def run_export(config=None):
        # Construct master MAF from all individual MAFs
        builder = MAFBuilder(self.config, self.sqlContext)
        df = builder.build()

        if 'case_centric' in self.config.indices:
            CaseCentricBuilder(self.config, self.sqlContext).build().load()

