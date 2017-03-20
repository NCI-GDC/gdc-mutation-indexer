import logging
logging.basicConfig()

from pyspark.sql.functions import lit, struct, collect_list

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
from exports.mappers import GeneMapper


class GeneCentricBuilder(BaseBuilder):
    """
    Builds gene-centric dataframe given case and maf dataframes::

        gene{}
             |___ case[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
    """

    index_name = 'gene_centric'
    id_field = 'gene_id'
    mapper = GeneMapper

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build(maf_df)

        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, obs_df)

        # Aggregating SSM
        self.log('Aggregating ssm by case_id and gene_id')
        ssm_df = (
                    ssm_df.select('gene_id',
                                  'case_id',
                                  struct(*ssm_df.drop('gene_id')
                                                .drop('case_id').columns)
                                  .alias('ssm')
                                  )
                          .groupBy(['gene_id', 'case_id'])
                          .agg(collect_list('ssm').alias('ssm'))
                 )
        return ssm_df

    def build_case_with_gene_id(self, maf_df):
        self.log('\nSelecting Gene from MAF')
        gene_df = (get_gene_df(maf_df, add_fields=['case_id'])
                   .select('case_id', 'gene_id'))

        self.log("Building Case")
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Getting gene_id for each case via joining with gene_df')
        case_gene_id = (
                     gene_df.join(case_df, on='case_id')
                            .select('gene_id', *case_df.columns)
                    )
        return case_gene_id

    def build_case_ssm(self, maf_df):
        case_gene_id = self.build_case_with_gene_id(maf_df)

        ssm_df = self.build_ssm(maf_df)
        self.log_count(ssm_df)

        self.log("Joining Case+gene_id with SSM [inner, gene_id, case_id]")
        case_ssm = (
                    case_gene_id.join(ssm_df, how='inner',
                                      on=['case_id', 'gene_id'])
                                .select('case_id', 'gene_id',
                                        struct('ssm',
                                               *case_gene_id.drop('gene_id').columns)
                                        .alias('case'))
                    )
        self.log_count(case_ssm)

        return case_ssm

    def build(self, maf_df):
        """
        """
        self.log('Building GeneCentric')
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.gene_centric = self.get_existing()
            if self.gene_centric is not None:
                return self

        self.log('Selecting Gene from MAF')
        gene_df = get_gene_df(maf_df, unique_fields=['gene_id'])

        case_ssm = self.build_case_ssm(maf_df)

        case_ssm_grouped = (
            case_ssm.groupBy(case_ssm.gene_id.alias('gene_id'))
            .agg(collect_list('case').alias('case'))
        )

        self.log('Joining Gene with Case [inner, "gene_id"]')
        gene_centric = (
            gene_df.join(case_ssm_grouped,
                         gene_df.gene_id == case_ssm_grouped.gene_id,
                         'inner')
                   .drop(case_ssm.case_id)
                   .drop(case_ssm_grouped.gene_id)
        )

        self.log_count(gene_centric)
        self.gene_centric = gene_centric
        self.log_count(self.gene_centric)

        path = self.config.index_paths[self.index_name]
        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self
