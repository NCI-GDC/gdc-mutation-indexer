from elasticsearch import Elasticsearch
from config import BaseConfig
from builders import MAFBuilder


class GDCMutationExport(object):
    '''
    The main entry point into the index export process for the mutation indices
    '''

    def __init__(self, sc, sqlContext, config=BaseConfig):
        self.config = config
        self.sc = sc
        self.sqlContext = sqlContext

        self.config.indices = { k: self.get_index_prefix(v)
                                for k,v in self.config.index_names.items()
                                if v is not None }
    
    def run_export(config):
        # Construct master MAF from all individual MAFs
        builder = MAFBuilder(self.config, self.sqlContext)
        df = builder.build()

    def get_index_prefix(self, index_name):
        '''
        Uses the version specified in the config, or will resolve the next
        version number by looking for an existing index and incrementing by one

        Eg:
            No indices exist in ES:
                index_name='case_centric' -> gdc_r0_case_centric

            gdc_r1_case_centric and gdc_r6_case_centric exist in ES:
                index_name='case_centric' -> gdc_r7_case_centric
        '''
        es = Elasticsearch(self.config.es_host, port=self.config.es_port)
        indices = es.indices.get_alias().keys()
        versions = [ int(v.split('_')[1].replace('r',''))
                        for v in indices if index_name in v and '_' in v ]
        # If there is no index with this name in it
        if versions == []:
            version = 0
        else:
            version = max(versions) + 1


        prefix = 'gdc_r{}_{}'.format(version, index_name)
        return prefix
