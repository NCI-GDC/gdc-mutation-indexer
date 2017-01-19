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
        # Construct master MAF from all individual MAFs
        urls = ['s3a://test/258c6357-4348-4b95-a266-03f50d862d9f/TCGA.KICH.somaticsniper.c652b1a7-2c9a-4d38-b317-c401b396a73e.somatic.maf.gz']
        builder = MAFBuilder(self.config, self.sqlContext)

        df = builder.build()
        print '\nPASSED MAFBuilder\n'

        #CaseCentricBuilder(self.config, self.sqlContext).build(df).load()
        GeneCentricBuilder(self.config, self.sqlContext).build(df).load()

