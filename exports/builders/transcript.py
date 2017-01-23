import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import explode, udf, col, collect_list, struct, lit
from pyspark.sql.types import ArrayType, StringType

from exports.builders.utils import transcript_id_udf, struct_select


class TranscriptBuilder(object):
    '''
    Build transcripts for each ssm by joining in data from the gene model
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        

    def build(self, maf_df):
        '''
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id
        '''
        ann_df = maf_df.select('transcript_id', 'consequence_type',
                                    struct(*struct_select('annotation.yml'))
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
                .select('transcript_id', struct('annotation', *struct_select('transcript.yml')).alias('transcript'))

        df = maf_df.join(tran_df, maf_df.transcript_id == tran_df.transcript_id)\
                    .select('ssm_id', struct('transcript').alias('transcript'))\
                    .groupby('ssm_id')\
                    .agg(collect_list('transcript').alias('consequence'))

        return df
