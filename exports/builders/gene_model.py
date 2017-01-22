import json

import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, regexp_extract
   

class GeneModelBuilder(object):
    """
    Constructs a Gene Model dataframe from ICGC's gene model json
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
        Loads the gene model json file
        '''
        df = self.sqlContext.read.json(self.config.gene_model_path)
        # Flatten, the mapping will re-introduce the structure
        df = df.select(col('external_db_ids.*'), *df.drop('external_db_ids').columns)
        return df
