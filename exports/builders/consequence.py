import logging
from pyspark.sql.functions import (explode,
                                   col,
                                   collect_list,
                                   collect_set,
                                   struct,
                                   lit,
                                   when,
                                   concat_ws)
from exports.builders.utils import (extract_rows_udf,
                                    all_effects_udf,
                                    struct_select,
                                    uuid5_col,
                                    extract_aas_position,
                                    extract_sift_polyphen,
                                    sanitize_aa_change,
                                    convert_empty_str_to_null_in_col,
                                    sanitize_gene_aa_change,
                                    )
from .df_builders import get_annotation_df, get_gene_df, get_transcript_df

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class ConsequenceBuilder(object):
    """
    Build transcripts for each ssm by joining in data from the gene model
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build_for_ssm(self, maf_df, index_name,
                      join_gene=False, add_gene_aa_change=False):
        """
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id

        :param maf_df: The formatted MAF dataframe from MAFBuilder
        :param index_name: name of the index this consequence is a part of
        :param join_gene: Whether or not to join the gene model to the
                          consquence. SSM and SSM Occurrence have gene under
                          consequences, while Case and Gene do not.
        """

        # => {gene_id, ssm_id, transcript_id,
        # empty, canonical_tracript_id, is_canonical,
        # do_not_us, consequence_type, aa_change
        # refs_seq_accession}
        ssm_tran = self.build_all_effects_cols(maf_df)

        ann_df = get_annotation_df(ssm_tran, index_name, add_fields=['ssm_id'],
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

    def build_for_cnv(self, gistic_df, index_name):
        """
        For now this is just gene information:

        consequence[]
                |_____ gene{}
        """

        # Create gene structure
        cons_df = (
            gistic_df.select(
                'cnv_id',
                struct(*struct_select(index_name, 'consequence'))
                .alias('consequence')
            ).groupby('cnv_id')
             .agg(collect_set('consequence').alias('consequence'))
        )

        return cons_df

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
        # Convert all_effects column into an array.
        # Each element corresponds to a transcript and it's effects
        ssm_tran = maf_df.withColumn('all_effects',
                                     extract_rows_udf()(col('all_effects'))
                                     .alias('all_effects'))

        effects_legend = [
            'do_not_use', 'consequence_type', 'aa_change', 'transcript_id',
            'ref_seq_accession', 'hgvsc', 'vep_impact', 'is_canonical',
            'SIFT', 'PolyPhen', 'Transcript_Strand',
        ]

        # Before exploding, let's save the transcript_id of the selected transcript
        ssm_tran = ssm_tran.withColumn('selected_transcript_id',
                                       col('transcript_id'))

        # Explode all_effects, to have each individual transcript data on a separate line
        # NOTE: after exploding, missing fields for secondary transcripts will be populated
        # with values from selected transcript (top level columns)
        ssm_tran = ssm_tran.select(explode('all_effects').alias('all_effects'),
                                   *ssm_tran.drop('all_effects').columns)

        # Extract transcripts' effects from 'all_effects'
        for idx, field in enumerate(effects_legend):
            ssm_tran = ssm_tran.withColumn(
                field, all_effects_udf(idx)(col('all_effects'))
            )

        # Clear the fields that we shouldn't copy from selected transcript (top level of maf_df)
        must_be_none_for_non_selected = [
            'amino_acids', 'cdna_position', 'cds_end', 'cds_length',
            'cds_position', 'cds_start', 'clin_sig', 'codons', 'domains',
            'ensp', 'hgvsp', 'hgvsp_short', 'protein_position',
            'swissprot', 'trembl', 'uniparc',
        ]
        for field in must_be_none_for_non_selected:
            ssm_tran = ssm_tran.withColumn(
                field,
                when(col('transcript_id') == col('selected_transcript_id'),
                     col(field))
                .otherwise(None)
            )

        # Take out the transcripts from genes that this mutation is not in
        ssm_tran = ssm_tran.filter("symbol == do_not_use")

        # get is_canonical
        ssm_tran = ssm_tran.withColumn(
            'is_canonical',
            ssm_tran.canonical_transcript_id == ssm_tran.transcript_id
        )

        ssm_tran = sanitize_aa_change(ssm_tran)
        # Get aas columns from aa_change
        ssm_tran = extract_aas_position(ssm_tran)
        ssm_tran = convert_empty_str_to_null_in_col(ssm_tran, 'aa_change')

        # Extract sift, polyphen columns
        ssm_tran = extract_sift_polyphen(ssm_tran)

        # Drop used helper columns
        for column in ['all_effects', 'do_not_use', 'symbol']:
            ssm_tran = ssm_tran.drop(column)

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

