import logging
from pyspark.sql.functions import explode, col, collect_list, struct, lit
from exports.builders.utils import extract_rows_udf, all_effects_udf, uuid5_col, extract_aas_position
from .df_builders import get_annotation_df, get_gene_df, get_transcript_df
logging.basicConfig()


class ConsequenceBuilder(object):
    """
    Build transcripts for each ssm by joining in data from the gene model
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self, maf_df, join_gene=False):
        """
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id

        :param maf_df: The formatted MAF dataframe from MAFBuilder
        :param join_gene: Whether or not to join the gene model to the
                          consquence. SSM and SSM Occurrence have gene under
                          consequences, while Case and Gene do not.
        """
        ann_df = get_annotation_df(maf_df, add_fields=['ssm_id'],
                                   unique_fields=['ssm_id', 'transcript_id'])
        ann_df = ann_df.select('ssm_id', 'transcript_id',
                               struct(ann_df.drop('ssm_id').columns)
                               .alias('annotation'))

        # => {gene_id, ssm_id, transcript_id,
        # empty, canonical_tracript_id, is_canonical,
        # do_not_us, consequence_type, aa_change
        # refs_seq_accession}
        ssm_tran = self._build_all_effects_cols(maf_df)

        # => {gene_id, ssm_id, transcrpt_id,
        # is_canonical,
        # do_not_us, consequence_type, aa_change,
        # refs_seq_accession}
        tran_df = get_transcript_df(ssm_tran, add_fields=['gene_id', 'ssm_id'])

        # {*fields} => {*fields, annotation: {}}
        tran_with_ann = tran_df.join(ann_df, on=['ssm_id', 'transcript_id'],
                                     how='left')

        if join_gene:
            # Build and join the gene if required
            gene_df = self._build_gene_struct(maf_df)

            # => {ssm_id, transcript_id, *transcript_fields, gene:{}}
            tran_with_ann = (
                tran_with_ann.join(gene_df, on='gene_id'))

        # => {ssm_id, consequence {transcript:
        #       {transcript_id, *transcript_fields}}}
        tran_with_ann = tran_with_ann.drop('gene_id').drop('empty')

        # Add consequence_id, a uuid from ssm_id and transcript_id
        tran_df = tran_with_ann.withColumn('consequence_id',
                                           uuid5_col(
                                               lit('ssm_consequence'),
                                               col('ssm_id'),
                                               col('transcript_id')))
        tran_df = tran_df.select(
                'ssm_id',
                struct(
                    'consequence_id',
                    struct(*tran_df.drop('ssm_id').drop('consequence_id'))
                    .alias('transcript')
                ).alias('consequence'))

        df = tran_df.groupby('ssm_id').agg(
            collect_list('consequence').alias('consequence'))

        return df

    def _build_all_effects_cols(self, maf_df):
        """
        Extracts information about transcripts from the all_effects column

        all_effects is formated as such:

        do_not_use,consequence_type,aa_change,transcript_id,refs_seq_accession;
        MORN1,synonymous_variant,p.=,ENST00000378531,NM_024848.1;
        MORN1,synonymous_variant,p.=,ENST00000378529,NM_001301060.1;

        We need to first extract each row within this column and explode it into
        a new row in the dataframe. We then extract each column from that row
        using the all_effects_udf

        There are some mutations that have transcripts not belonging to the
        gene of that mutation. They can be identified by matching the symbol
        from the mutation to the do_not_use column. These should be removed.
        """
        # Extract columns from the all_effects column
        ssm_tran = maf_df.select('gene_id', 'ssm_id', 'symbol',
                                 'all_effects', 'canonical_transcript_id')
        # Turn each row within in the all_effects column into rows in the df
        ssm_tran = ssm_tran.withColumn('all_effects',
                                       extract_rows_udf()(col('all_effects'))
                                       .alias('all_effects'))
        # Now extract columns within all_effects to columns in the df
        fields = {
            'do_not_use': 0,
            'consequence_type': 1,
            'aa_change': 2,
            'transcript_id': 3,
            'ref_seq_accession': 4
        }
        ssm_tran = ssm_tran.select('gene_id', 'ssm_id', 'symbol',
                                   'canonical_transcript_id',
                                   explode('all_effects').alias('all_effects'))

        for field, idx in fields.items():
            ssm_tran = ssm_tran.withColumn(field,
                                           all_effects_udf(idx)(col('all_effects')))
        ssm_tran = ssm_tran.drop('all_effects')
        # Take out the transcripts from genes that this mutation is not in
        ssm_tran = ssm_tran.filter("symbol == do_not_use")
        ssm_tran = ssm_tran.drop('do_not_use').drop('symbol')

        # get is_canonical
        ssm_tran = ssm_tran.withColumn(
            'is_canonical',
            ssm_tran.canonical_transcript_id == ssm_tran.transcript_id)

        # Get aas columns from aa_change
        ssm_tran = extract_aas_position(ssm_tran)

        return ssm_tran

    def _build_gene_struct(self, maf_df):
        # Build and join the gene if required
        gene_df = get_gene_df(
            maf_df,
            drop_fields=['transcripts', 'description',
                         'canonical_transcript_length',
                         'canonical_transcript_length_cds',
                         'canonical_transcript_length_genomic',
                         'gene_strand', 'name', 'biotype'])
        gene_struct_df = gene_df.select(
            'gene_id', struct(col('*')).alias('gene'))
        return gene_struct_df
