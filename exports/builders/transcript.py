import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import struct

from exports.builders.utils import ssm_uuid_udf


class TranscriptBuilder(object):
    '''
    Builds transcript dataframe from the maf dataframe
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        

    def build(self, df):
        '''
        '''
        # Rename columns
        df = df.select('ssm_id',*( maf_transcript_map.keys() + maf_annotation_map.keys() ))
        # Select annotation into nested format
        tran_anno_df = tran_anno_df.select(struct(*maf_annotation_map.keys()).alias('annotation'),
                                            'ssm_id', *maf_transcript_map.keys())\
                                   .drop_duplicates()
                                   #tran_anno_df.printSchema()
