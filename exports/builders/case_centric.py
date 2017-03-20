from pyspark.sql.functions import struct, collect_list, udf, size, col
from pyspark.sql.types import BooleanType

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
    id_field = 'case_id'
    mapper = CaseMapper

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build(maf_df)

        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, obs_df)

        # Aggregate SSM
        self.log('Aggregating ssm by case_id and gene_id')
        ssm_df = (
            ssm_df.select(
             'gene_id', 'case_id',
             struct(*ssm_df.drop('gene_id')
                           .drop('case_id').columns).alias('ssm'))
            .groupBy(['gene_id', 'case_id'])
            .agg(collect_list('ssm').alias('ssm')))

        return ssm_df

    def build_gene_ssm(self, maf_df):
        self.log('Building Gene from MAF')
        gene_df = get_gene_df(maf_df,
                              add_fields=['case_id'],
                              drop_fields=['canonical_transcript_length',
                                           'canonical_transcript_length_cds',
                                           'canonical_transcript_length_genomic'])
        self.log_count(gene_df)

        ssm_df = self.build_ssm(maf_df)
        self.log_count(ssm_df)

        self.log('Join ssm with Gene [inner, gene_id, case_id]')
        gene_ssm = (
            gene_df.join(ssm_df, on=['gene_id', 'case_id'], how='left')
            .select('case_id',
                    struct('ssm', *gene_df.drop('case_id').columns)
                    .alias('gene')))

        self.log_count(gene_ssm)

        return gene_ssm

    def add_ssm_tested(self, df):
        """
        Evaluates whether the case was tested for ssm or not
        For now, it is acceptable to say that any case with > 0 genes
        was tested
        """
        return (df.withColumn('_ngenes', size(col('gene')))
                    .withColumn('ssm_tested',
                            udf(lambda x: x > 0, BooleanType())(col('_ngenes')))
                    .drop('_ngenes'))

    def build(self, maf_df):
        self.log('Building Case')
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.case_centric = self.get_existing()
            if self.case_centric is not None:
                return self

        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        gene_ssm = self.build_gene_ssm(maf_df)

        gene_ssm_grouped = (
            gene_ssm.groupBy(gene_ssm.case_id)
            .agg(collect_list('gene').alias('gene')))

        self.log('Final join Case with last join result [inner, submitter_id]')
        case_centric = (
            case_df.join(gene_ssm_grouped, on=['case_id'], how='left')
        )
        case_centric = self.add_ssm_tested(case_centric)
        self.case_centric = case_centric

        # Truncate outliers
        threshold = self.config.percentile_threshold['genes_per_case']
        self.case_centric = self.truncate_df_at_percentile(case_centric, 'gene',
                                                           threshold)

        self.log_count(self.case_centric)
        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self
