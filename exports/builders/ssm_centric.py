import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, regexp_extract

from exports.builders.utils import ssm_uuid_udf


class SSMCentric(object):
    '''
    Builds ssm-centric dataframe given case and maf dataframes
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self):
        '''
        '''
        pass
