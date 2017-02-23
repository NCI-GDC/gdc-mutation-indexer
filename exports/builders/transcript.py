import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import explode, udf, col, collect_list, struct, lit, size
from pyspark.sql.types import ArrayType, StringType

from exports.builders.utils import struct_select, extract_rows_udf, all_effects_udf


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
        ann_df = maf_df.select(*struct_select('annotation.yml'))\
                                .drop_duplicates(['transcript_id'])

        ssm_tran = maf_df.select('ssm_id', 'all_effects', 'canonical_transcript_id')\
                .withColumn('all_effects',extract_rows_udf()(col('all_effects')).alias('all_effects'))\
                .select('ssm_id', 'canonical_transcript_id', explode('all_effects').alias('all_effects'))\
                .withColumn('do_not_keep', all_effects_udf(0)(col('all_effects')))\
                .withColumn('consequence_type', all_effects_udf(1)(col('all_effects')))\
                .withColumn('aa_change', all_effects_udf(2)(col('all_effects')))\
                .withColumn('transcript_id', all_effects_udf(3)(col('all_effects')))\
                .withColumn('ref_seq_accession', all_effects_udf(4)(col('all_effects')))\
                .drop('all_effects')

        tran_df = maf_df.select(explode('transcripts')
                                .alias('transcript'),
                                'gene_id', 'symbol', 'empty')\
                        .select('transcript.*', 'gene_id', 'empty', 'symbol')\
                        .select(col('*'), col('id').alias('transcript_id'),
                            'gene_id','empty', 'symbol')\
                        .drop('id')\
                        .select('transcript_id', 'gene_id', 'symbol', 'empty')\
                        .join(ssm_tran, on='transcript_id')\
                        .select('gene_id', 'transcript_id',
                                struct(*struct_select('transcript.yml'))\
                                    .alias('transcript'))

        tran_ann = tran_df.join(ann_df, on='transcript_id', how='left')\
                    .select('transcript_id', 'gene_id',
                            struct(ann_df.columns).alias('annotation'))

        if join_gene:
            # Build and join the gene if required
            gene_df = maf_df.select(*struct_select('gene.yml',
                                                    ignore=['transcripts']))\
                            .drop('transcripts')\
                            .drop('description')\
                            .drop('canonical_transcript_length_genomic')\
                            .drop('canonical_transcript_length_cds')\
                            .drop('gene_strand')\
                            .select('gene_id', struct(col('*')).alias('gene'))

            tran_df = tran_ann.join(gene_df, on='gene_id')\
                                    .drop('gene_id')\
                                    .join(ssm_tran, on='transcript_id')\
                                    .select('ssm_id', struct(
                                                        struct('*')
                                                        .alias('transcript'))
                                                      .alias('transcript'))
        else:
            # Just skip the gene otherwise
            tran_df = tran_ann.join(ssm_tran, on='transcript_id')\
                                .select('ssm_id',
                                    struct(
                                        struct('*')
                                        .alias('transcript'))
                                    .alias('transcript'))

        df = tran_df.groupby('ssm_id').agg(collect_list('transcript').alias('consequence'))

        return df
