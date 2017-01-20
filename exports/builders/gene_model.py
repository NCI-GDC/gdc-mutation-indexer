import json

import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, regexp_extract

from exports.builders.utils import ssm_uuid_udf
   

class GeneModelBuilder(object):
    """
    """
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
        # TODO: filename should be in self.config
        df = self.sqlContext.read.json("/home/ubuntu/Projects/gdc-mutation-indexer/exports/data/gene_model.json")    
        return df

