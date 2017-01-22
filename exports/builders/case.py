import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, regexp_extract


class CaseBuilder(object):
    '''
    Builds a case dataframe by loading case documents from gdc_from_graph
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self):
        '''
        '''
        df = self.load()
        return df

    def load(self):
        '''
        Loads case docs from the gdc_from_graph index into a dataframe
        '''
        source = '{}/{}'.format( self.config.graph_index, self.config.graph_document)

        return self.sqlContext.read.format("es")\
            .option('es.nodes', self.config.source_es_host)\
            .option('es.nodes.resolve.hostname','false')\
            .option('es.read.field.include', self.config.case_fields)\
            .option('es.read.field.as.array.include', self.config.case_arrays)\
            .option('es.resource.read', source)\
            .load(source)
