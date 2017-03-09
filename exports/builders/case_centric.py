import json

from pyspark.sql.functions import struct, collect_list

from exports.builders.df_builders import (
    get_gene_df,
    build_ssm_subtree,
)
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders import BaseBuilder
from exports.mappers import CaseMapper


class CaseCentricBuilder(BaseBuilder):
    """
    Builds case-centric dataframe given case and maf dataframes::

        case{}
             |___ gene[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
    """

    index_name = 'case_centric'

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build(maf_df)

        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, obs_df)
        return ssm_df

    def build_gene_ssm(self, maf_df):
        self.log('Building Gene from MAF')
        gene_df = get_gene_df(maf_df,
                              add_fields=['_case_submitter_id'],
                              drop_fields= ['canonical_transcript_length',
                                        'canonical_transcript_length_cds',
                                        'canonical_transcript_length_genomic'])
        self.log_count(gene_df)

        ssm_df = self.build_ssm(maf_df)

        self.log('Aggregating ssm by _case_submitter_id and gene_id')
        ssm_df = (
            ssm_df.select(
             'gene_id', '_case_submitter_id',
             struct(*ssm_df.drop('gene_id')
                    .drop('_case_submitter_id')
                    .columns).alias('ssm'))
            .groupBy(['gene_id', '_case_submitter_id'])
            .agg(collect_list('ssm').alias('ssm')))
        self.log_count(ssm_df)

        self.log('Join ssm with Gene [inner, gene_id, _case_submitter_id]')
        join_condition = (
            (gene_df.gene_id == ssm_df.gene_id) &
            (gene_df._case_submitter_id == ssm_df._case_submitter_id))

        gene_ssm = (
            gene_df.join(ssm_df, join_condition)
            .drop(ssm_df.gene_id)
            .drop(ssm_df._case_submitter_id)
            .select('_case_submitter_id',
                    struct('ssm', *gene_df.drop('_case_submitter_id').columns)
                    .alias('gene')))
        self.log_count(gene_ssm)
        return gene_ssm

    def build(self, maf_df=None):
        '''
        '''
        self.log('\nBuilding CaseCentric')
        if maf_df is None:
            self.logger.info('Building MAF')
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        gene_ssm = self.build_gene_ssm(maf_df)

        gene_ssm_grouped = (
            gene_ssm.groupBy(gene_ssm._case_submitter_id)
            .agg(collect_list('gene').alias('gene')))

        self.log('Final join Case with last join result [inner, submitter_id]')
        case_centric = (
            case_df.join(gene_ssm_grouped,
                         case_df.submitter_id == gene_ssm_grouped._case_submitter_id,
                         'inner')
            .drop(gene_ssm_grouped._case_submitter_id))
        self.case_centric = case_centric
        # Truncate outliers
        self.case_centric = self.truncate_df_at_percentile(case_centric, 'gene', self.config.percentile_threshold['genes_per_case'])
        self.log_count(case_centric)

        self.log('Build finished')
        return self

    def load(self):
        '''
        '''
        index = self.config.indices['case_centric']
        doc = self.config.index_names['case_centric']
        settings = json.dumps(CaseMapper(doc).settings)

        self.load_to_elasticsearch(index, doc, settings, self.case_centric, 'case_id')
