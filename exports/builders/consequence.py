import logging
from pyspark.sql.functions import (explode,
                                   col,
                                   collect_list,
                                   struct,
                                   lit,
                                   when,
                                   concat_ws)
from exports.builders.utils import (extract_rows_udf,
                                    all_effects_udf,
                                    uuid5_col,
                                    extract_aas_position,
                                    extract_impact_or_score,
                                    sanitize_aa_change,
                                    convert_empty_str_to_null_in_col,
                                    sanitize_gene_aa_change,
                                    )
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

    def build(self, maf_df, index_name, join_gene=False, add_gene_aa_change=False):
        """
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id

        :param maf_df: The formatted MAF dataframe from MAFBuilder
        :param join_gene: Whether or not to join the gene model to the
                          consquence. SSM and SSM Occurrence have gene under
                          consequences, while Case and Gene do not.
        """

        # => {gene_id, ssm_id, transcript_id,
        # empty, canonical_tracript_id, is_canonical,
        # do_not_us, consequence_type, aa_change
        # refs_seq_accession}
        ssm_tran = self.build_all_effects_cols(maf_df)

        ann_df = get_annotation_df(maf_df, index_name, add_fields=['ssm_id'],
                                   unique_fields=['ssm_id', 'transcript_id'])
        ann_df = ann_df.select('ssm_id', 'transcript_id',
                               struct(ann_df.drop('ssm_id').columns)
                               .alias('annotation'))
        # => {gene_id, ssm_id, transcrpt_id,
        # is_canonical,
        # do_not_us, consequence_type, aa_change,
        # refs_seq_accession}

        tran_df = get_transcript_df(ssm_tran, index_name,
                                    add_fields=['gene_id', 'ssm_id'])

        # {*fields} => {*fields, annotation: {}}
        tran_with_ann = tran_df.join(ann_df, on=['ssm_id', 'transcript_id'],
                                     how='left')

        # gene_aa_change cannot be added if gene is not joined:
        if add_gene_aa_change:
            join_gene = True

        if join_gene:
            # Build and join the gene if required
            gene_df = self._build_gene_struct(maf_df, index_name)

            # => {ssm_id, transcript_id, *transcript_fields, gene:{}}
            tran_with_ann = (tran_with_ann.join(gene_df, on='gene_id'))

        # => {ssm_id, consequence {transcript:
        #       {transcript_id, *transcript_fields}}}
        tran_with_ann = tran_with_ann.drop('gene_id').drop('empty')

        # Add consequence_id, a uuid from ssm_id and transcript_id
        tran_df = tran_with_ann.withColumn('consequence_id',
                                           uuid5_col(lit('ssm_consequence'),
                                                     col('ssm_id'),
                                                     col('transcript_id')))
        if add_gene_aa_change:
            tran_df = (tran_df.withColumn('gene_aa_change',
                                          when(col("gene.symbol").isNull()
                                               | col("aa_change").isNull(),
                                               None)
                                          .otherwise(concat_ws(' ',
                                                     tran_df.gene.symbol,
                                                     tran_df.aa_change))))
            tran_df = tran_df.select('ssm_id',
                                     struct('consequence_id',
                                            struct(*tran_df
                                                   .drop('ssm_id')
                                                   .drop('consequence_id')
                                                   .drop('gene_aa_change'))
                                            .alias('transcript'))
                                     .alias('consequence'),
                                     'gene_aa_change')

            df = (tran_df.groupby('ssm_id')
                         .agg(collect_list('consequence').alias('consequence'),
                              collect_list('gene_aa_change').alias('gene_aa_change')))
            df = sanitize_gene_aa_change(df)

        else:
            tran_df = tran_df.select('ssm_id',
                                     struct('consequence_id',
                                            struct(*tran_df
                                                   .drop('ssm_id')
                                                   .drop('consequence_id'))
                                            .alias('transcript'))
                                     .alias('consequence'))

            df = (tran_df.groupby('ssm_id')
                         .agg(collect_list('consequence').alias('consequence')))

        return df

    @staticmethod
    def build_all_effects_cols(maf_df):
        """
        Extracts information about transcripts from the all_effects column

        all_effects is formated as such:

        BEFORE:
        do_not_use,consequence_type,aa_change,transcript_id,refs_seq_accession;
        MORN1,synonymous_variant,p.=,ENST00000378531,NM_024848.1;

        NEW all_effects fields: (appended after old ones)
        HGVSc,IMPACT,CANONICAL,SIFT,PolyPhen,Transcript_Strand
        c.3602T>G,MODERATE,YES,tolerated(0.06),possibly_damaging(0.614),1

        We need to first extract each row within this column and explode it into
        a new row in the dataframe. We then extract each column from that row
        using the all_effects_udf

        There are some mutations that have transcripts not belonging to the
        gene of that mutation. They can be identified by matching the
        symbol from the mutation to the do_not_use column.
        These should be removed.
        """
        # Extract columns from the all_effects column
        ssm_tran = maf_df.withColumn('all_effects',
                                     extract_rows_udf()(col('all_effects'))
                                     .alias('all_effects'))

        effects_legend = ['do_not_use', 'consequence_type', 'aa_change',
                          'transcript_id', 'ref_seq_accession', 'HGVSc',
                          'IMPACT', 'CANONICAL', 'SIFT', 'PolyPhen',
                          'Transcript_Strand']

        effects_to_keep = ['do_not_use', 'consequence_type', 'aa_change',
                           'transcript_id', 'ref_seq_accession', 'PolyPhen',
                           'SIFT']

        # Now extract columns within all_effects to columns in the df
        fields = {name: ix for ix, name in enumerate(effects_legend)
                  if name in effects_to_keep}

        ssm_tran = ssm_tran.select(explode('all_effects').alias('all_effects'),
                                   *ssm_tran.drop('all_effects').columns)

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

        ssm_tran = sanitize_aa_change(ssm_tran)
        # Get aas columns from aa_change
        ssm_tran = extract_aas_position(ssm_tran)
        ssm_tran = convert_empty_str_to_null_in_col(ssm_tran, 'aa_change')
        
        # Extract '{polyphen|sift}_{impact|score}':
        ssm_tran = extract_impact_or_score(ssm_tran, 'PolyPhen', 'impact', 'polyphen_impact')
        ssm_tran = extract_impact_or_score(ssm_tran, 'PolyPhen', 'score', 'polyphen_score')
        ssm_tran = extract_impact_or_score(ssm_tran, 'SIFT', 'impact', 'sift_impact')
        ssm_tran = extract_impact_or_score(ssm_tran, 'SIFT', 'score', 'sift_score')
        ssm_tran = ssm_tran.drop('PolyPhen').drop('SIFT')

        return ssm_tran

    def _build_gene_struct(self, maf_df, index_name):
        # Build and join the gene if required

        to_drop = ['transcripts', 'description', 'canonical_transcript_length',
                   'name', 'canonical_transcript_length_cds',
                   'canonical_transcript_length_genomic']

        gene_df = get_gene_df(maf_df, index_name, drop_fields=to_drop)
        gene_struct_df = gene_df.select('gene_id',
                                        struct(col('*')).alias('gene'))
        return gene_struct_df
