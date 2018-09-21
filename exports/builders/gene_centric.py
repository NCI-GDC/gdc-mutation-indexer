import logging

from pyspark.sql.functions import struct, collect_list

from exports.builders.df_builders import (
    get_gene_df,
    build_ssm_subtree,
    build_cnv_subtree,
)
from exports.builders import (
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders import BaseBuilder

logging.basicConfig()


class GeneCentricBuilder(BaseBuilder):
    """
    Builds gene-centric dataframe given case and maf dataframes::

        gene{}
             |___ case[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
    """

    index_name = 'gene_centric'
    id_field = 'gene_id'

    def build(self, maf_df, gistic_df, case_df):
        """
        Builds Gene Centric index
        """
        self.log('Building GeneCentric')
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.gene_centric = self.get_existing()
            if self.gene_centric is not None:
                return self

        self.log('Building Gene')
        gene_df = get_gene_df(maf_df, self.index_name,
                              unique_fields=['gene_id'])
        self.log_count(gene_df)

        self.log('Building Case subtree')
        case_subtree = self.build_case_subtree(maf_df, gistic_df, case_df)

        self.log('Joining Gene with Case subtree [inner, "gene_id"]')
        gene_centric = (
            gene_df.join(case_subtree,
                         gene_df.gene_id == case_subtree.gene_id,
                         'inner')
                   .drop(case_subtree.gene_id)
        )
        self.log_count(gene_centric)

        self.gene_centric = gene_centric
        self.log('Build finished')

        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_case_subtree(self, maf_df, gistic_df, case_df):
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """
        self.log('Building Case with gene info from MAF and GeneModel')
        case_and_gene_df = self._build_case_with_gene_id(maf_df, gistic_df)

        self.log('Building SSM subtree')
        ssm_df = self.build_ssm_subtree(maf_df)
        self.log_count(ssm_df)

        self.log('Building CNV subtree')
        cnv_df = self.build_cnv_subtree(gistic_df)
        self.log_count(cnv_df)

        self.log("Join SSM and CNV subtrees to Case [left, gene_id, case_id]")
        case_subtree = (
            case_and_gene_df.join(ssm_df,
                                  on=['gene_id', 'case_id'], how='left')
                            .join(cnv_df,
                                  on=['gene_id', 'case_id'], how='left')
                            .select('gene_id',
                                    struct('ssm',
                                           'cnv',
                                           *case_df.drop('gene_id').columns)
                                    .alias('case'))
        )
        self.log_count(case_subtree)

        self.log('Grouping by case_id and aggregating to list under "gene"')
        case_subtree = (
            case_subtree.groupBy(case_subtree.gene_id.alias('gene_id'))
            .agg(collect_list('case').alias('case'))
        )
        return case_subtree

    def build_ssm_subtree(self, maf_df):
        """
        TODO: This branch is same as in case_centric and can be reused
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """

        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build_for_ssm(maf_df,
                                                        self.index_name)

        # Observation
        obs_df = ObservationBuilder().build_for_ssm(maf_df, self.index_name)
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, self.index_name,
                                   obs_df=obs_df)

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

    def build_cnv_subtree(self, gistic_df):
        """
        TODO: This branch is same as in case_centric and can be reused
        cnv[]
           |___ consequence[]
           |            |_____ gene{}
           |___ observation[]

        """
        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build_for_cnv(gistic_df, self.index_name))

        # Observation
        obs_df = ObservationBuilder().build_for_cnv(gistic_df, self.index_name)

        # Build the final cnv dataframe
        cnv_df = build_cnv_subtree(gistic_df, cons_df,
                                   self.index_name, obs_df=obs_df)

        # Aggregate CNV
        self.log('Aggregating cnv by case_id and gene_id')
        cnv_df = (
            cnv_df.select(
             'gene_id', 'case_id',
             struct(*cnv_df.drop('gene_id')
                           .drop('case_id').columns).alias('cnv'))
            .groupBy(['gene_id', 'case_id'])
            .agg(collect_list('cnv').alias('cnv')))

        return cnv_df

    def _build_case_with_gene_id(self, maf_df, gistic_df, case_df):
        self.log('\nSelecting Gene from MAF')
        gene_df = (get_gene_df(maf_df, self.index_name, add_fields=['case_id'])
                   .select('case_id', 'gene_id'))

        self.log('Getting gene_id for each case via joining with gene_df')
        case_gene_id = (
                     gene_df.join(case_df, on='case_id')
                            .select('gene_id', *case_df.columns)
                    )
        return case_gene_id
