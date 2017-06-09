from pyspark.sql.functions import struct, collect_list, udf, size, col
from pyspark.sql.types import BooleanType, ArrayType, StringType

from exports.builders.df_builders import (
    get_gene_df,
    build_ssm_subtree,
)
from exports.builders import (
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders import BaseBuilder


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

    def build(self, maf_df):
        """
        Builds Case Centric index
        """
        self.log('Building Case')
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.case_centric = self.get_existing()
            if self.case_centric is not None:
                return self

        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        self.log_count(case_df)

        gene_ssm = self.build_gene_ssm(maf_df)

        gene_ssm_grouped = (
            gene_ssm.groupBy(gene_ssm.case_id)
                    .agg(collect_list('gene').alias('gene'))
        )

        self.log('Final join Case with last join result [inner, case_id]')
        case_centric = (
            case_df.join(gene_ssm_grouped,
                         on=['case_id'], how='left')
        )

        # Coerce any cases that didn't have variation data from None to []
        case_centric = case_centric.withColumn('available_variation_data',
                              udf(lambda x: [] if (x == None) else x,
                              ArrayType(StringType()))(col('available_variation_data')))
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

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(maf_df, self.index_name))

        # Observation
        obs_df = (ObservationBuilder(self.config, self.sqlContext)
                  .build(maf_df, self.index_name))
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, self.index_name,
                                   obs_df=obs_df)

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

        gene_df = get_gene_df(maf_df, self.index_name,
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
                    struct('ssm', *gene_df.drop('case_id')
                                          .columns)
                    .alias('gene'))
        )

        self.log_count(gene_ssm)

        return gene_ssm

