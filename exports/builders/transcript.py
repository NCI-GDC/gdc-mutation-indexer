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

        ssm_tran = self._build_ssm_tran(maf_df)

        tran_df = maf_df.select(explode('transcripts.id')
                                .alias('transcript_id'),
                                'gene_id', 'symbol', 'empty')\
            .join(ssm_tran, on='transcript_id')\
            .select('gene_id', 'transcript_id',
                    struct(*struct_select('transcript.yml'))\
                    .alias('transcript'))

        tran_ann = tran_df.join(ann_df, on='transcript_id', how='left')\
                    .select('transcript_id', 'gene_id',
                            struct(ann_df.columns).alias('annotation'))

        if join_gene:
            gene_df = self._build_gene_df(maf_df)
            tran_ann = tran_ann.join(gene_df, on='gene_id')\
                                    .drop('gene_id')\

        # Just skip the gene otherwise
        tran_all = tran_ann.join(ssm_tran, on='transcript_id')\
                            .select('ssm_id',
                                struct(
                                    struct('*')
                                    .alias('transcript'))
                                .alias('transcript'))

        df = tran_all.groupby('ssm_id').agg(collect_list('transcript').alias('consequence'))

        return df

    def _build_ssm_tran(self, maf_df):
        """
        Extracts information about transcripts from the all_effects column

        all_effects is formated as such:

        do_not_keep,consequence_type,aa_change,transcript_id,refs_seq_accession;
        MORN1,synonymous_variant,p.=,ENST00000378531,NM_024848.1;
        MORN1,synonymous_variant,p.=,ENST00000378529,NM_001301060.1;

        We need to first extract each row within this column and explode it into
        a new row in the dataframe. We then extract each column from that row
        using the all_effects_udf

        """
        # Extract columns from the all_effects column
        ssm_tran = maf_df.select('ssm_id', 'all_effects', 'canonical_transcript_id')
        # Turn each row within in the all_effects column into rows in the df
        ssm_tran = ssm_tran.withColumn('all_effects',
                                       extract_rows_udf()(col('all_effects'))
                                            .alias('all_effects'))
        # Now extract columns within all_effects to columns in the df
        ssm_tran = ssm_tran.select('ssm_id', 'canonical_transcript_id',
                                   explode('all_effects').alias('all_effects'))\
                            .withColumn('do_not_keep',
                                        all_effects_udf(0)(col('all_effects')))\
                            .withColumn('consequence_type',
                                        all_effects_udf(1)(col('all_effects')))\
                            .withColumn('aa_change',
                                        all_effects_udf(2)(col('all_effects')))\
                            .withColumn('transcript_id',
                                        all_effects_udf(3)(col('all_effects')))\
                            .withColumn('ref_seq_accession',
                                        all_effects_udf(4)(col('all_effects')))\
                            .drop('all_effects')

        return ssm_tran

    def _build_gene_df(self, maf_df):
        # Build and join the gene if required
        gene_df = maf_df.select(*struct_select('gene.yml',
                                                ignore=['transcripts']))\
                        .drop(['transcripts', 'description',
                               'canonical_transcript_length_genomic',
                               'canonical_transcript_length_cds',
                               'gene_strand'])\
                        .select('gene_id', struct(col('*')).alias('gene'))
        return gene_df
