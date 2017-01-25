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

    def build(self, maf_df, join_gene=False):
        '''
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id
        '''
        ann_df = maf_df.select('transcript_id', 'consequence_type', 'ssm_id',
                               struct(*struct_select('annotation.yml'))
                               .alias('annotation')) \
            .drop_duplicates(['transcript_id'])
        if join_gene:
            gene_df = maf_df.select('ssm_id',
                                    struct(*struct_select('gene.yml', ignore=['transcripts'])).alias('gene')) \
                     .drop_duplicates(['ssm_id'])

            gene_ann_df = ann_df.join(gene_df, ann_df.transcript_id == gene_df.canonical_transcript_id) \
                .drop_duplicates(['transcript_id'])
        else:
            gene_ann_df = ann_df
        # Explode the transcript_id array then join then group by (ssm_id)
        ssm_transcript = maf_df.select('ssm_id', 'all_effects') \
            .withColumn('transcript_ids', transcript_id_udf()(col('all_effects'))) \
            .select('ssm_id',
                    explode('transcript_ids').alias('transcript_id')) \
            .drop_duplicates(['transcript_id', 'ssm_id'])

        if join_gene:
            to_use = struct('annotation', 'gene', *struct_select('transcript.yml'))
        else:
            to_use = struct('annotation', *struct_select('transcript.yml'))

        tran_df = maf_df.join(gene_ann_df, maf_df.transcript_id == gene_ann_df.transcript_id) \
                .withColumn('empty', lit('').cast(StringType())) \
                .select('transcript_id', to_use.alias('transcript'))

        df = ssm_transcript.join(tran_df, ssm_transcript.transcript_id == tran_df.transcript_id) \
            .select('ssm_id', 'transcript') \
            .groupby('ssm_id') \
            .agg(collect_list('transcript').alias('consequence'))
        return df
