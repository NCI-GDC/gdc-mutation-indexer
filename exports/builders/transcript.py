import logging
from pyspark.sql.functions import explode, col, collect_list, struct, lit
from exports.builders.utils import struct_select, extract_rows_udf, all_effects_udf
logging.basicConfig()



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

        tran_ann = ssm_tran.join(ann_df, on='transcript_id', how='left')\
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
                        .drop('gene_id').drop('empty').drop('symbol')

        # Just skip the gene otherwise
        tran_df = tran_df.join(ssm_tran, on='transcript_id')\
            .select('ssm_id', struct(struct('*').alias('transcript')).alias('consequence'))

        df = tran_df.groupby('ssm_id').agg(collect_list('consequence').alias('consequence'))

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
        ssm_tran = maf_df.select('gene_id', 'symbol', 'empty', 'ssm_id',
                                    'all_effects', 'canonical_transcript_id')
        # Turn each row within in the all_effects column into rows in the df
        ssm_tran = ssm_tran.withColumn('all_effects',
                                       extract_rows_udf()(col('all_effects'))
                                            .alias('all_effects'))
        # Now extract columns within all_effects to columns in the df
        ssm_tran = ssm_tran.select('gene_id', 'symbol', 'empty', 'ssm_id',
                                    'canonical_transcript_id',
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
                        .drop('transcripts')\
                        .drop('description')\
                        .drop('canonical_transcript_length_genomic')\
                        .drop('canonical_transcript_length_cds')\
                        .drop('gene_strand')\
                        .select('gene_id', struct(col('*')).alias('gene'))
        return gene_df
