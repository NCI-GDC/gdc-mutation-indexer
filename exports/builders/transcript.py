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
        ann_df = maf_df.select('transcript_id', 'consequence_type',
                                    struct(*struct_select(
                                        self.config.mappings['annotation']))
                                        .alias('annotation'))\
                                        .drop_duplicates(['transcript_id'])

        # Explode the transcript_id array then join then group by (gene_id, ssm_id)
        maf_df = maf_df\
                   .select('ssm_id', 'all_effects')\
                   .withColumn('transcript_ids', transcript_id_udf()(col('all_effects')))\
                   .select('ssm_id', explode('transcript_ids').alias('transcript_id'))\
                   .drop_duplicates(['transcript_id', 'ssm_id'])

        # Load gene model and explode the transcripts
        tran_df = self.sqlContext.read.json(self.config.gene_model_path)\
                .select(col('*'), explode('transcripts').alias('transcript'))\
                .select(col('*'), 'transcript.*')\
                .withColumn('empty', lit('').cast(StringType()))

        tran_df = tran_df.join(ann_df, tran_df.id == ann_df.transcript_id)\
                         .select('transcript_id', struct('annotation',
                                                         *struct_select(
                                                             self.config.mappings['transcript']))
                                 .alias('transcript'))

        df = maf_df.join(tran_df, maf_df.transcript_id == tran_df.transcript_id)\
                   .select('ssm_id', struct('transcript').alias('transcript'))\
                   .groupby('ssm_id')\
                   .agg(collect_list('transcript').alias('consequence'))

        return df
