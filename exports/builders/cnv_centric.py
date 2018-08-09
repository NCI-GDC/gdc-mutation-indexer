import logging

from pyspark.sql.functions import struct, collect_list

from exports.builders import (
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.builders.df_builders import (
    get_single_df
)
logging.basicConfig()


class CNVCentricBuilder(BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case and maf dataframes::

        cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                    |_____ case{}
                                |____ observation[]
    """

    index_name = 'cnv_centric'
    id_field = 'cnv_id'

    def build(self, maf_df):
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric_df = self.get_existing()
            if self.cnv_centric_df is not None:
                return self

        # TODO: Load from gistic files
        # will not be in maf
        cnv_df = get_single_df(input_df=maf_df,
                               index_name=self.index_name,
                               mapping='cnv',
                               unique_fields=['cnv_id'])

        cons_df = self.build_consequence(maf_df)

        occurrence_df = self.build_occurrence(maf_df)

        ##############
        # LOGGING
        self.log('Final join CNV + Consequence + Occurrence')
        ###############

        cnv_centric_df = cnv_df.join(cons_df, on='cnv_id')\
                               .join(occurrence_df, on='cnv_id')

        # Truncate outliers
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        self.cnv_centric_df = self.truncate_df_at_percentile(cnv_centric_df,
                                                             'occurrence',
                                                             threshold)

        ###############
        # LOGGING
        self.log_count(self.cnv_centric_df)
        self.log('Build finished')
        ###############

        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_consequence(self, maf_df):
        """
        I have no idea what these options mean...
        join_gene, add_gene_aa_change?
        TODO: this is going to need to be a whole different kind
        of consequence
        """
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(input_df=maf_df,
                          index_name=self.index_name,
                          join_gene=True,
                          add_gene_aa_change=True))
        return cons_df

    def build_occurrence(self, maf_df):
        """
        Occurrence
        """
        # 1. Observation
        obs_df = self.build_observation(maf_df)

        # 2. Case
        case_df = self.build_case(maf_df)

        # 3. Join Case to Observation
        ###############
        # LOGGING
        self.log('Joining Cases with Observation, [right, case_id]')
        ###############

        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('ssm_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('ssm_id')
                         .agg(collect_list('occurrence').alias('occurrence')))

        ###############
        # LOGGING
        self.log_count(occurrence_df)
        ###############

        return occurrence_df

    def build_observation(self, maf_df):
        """
        Observation
        """
        ###############
        # LOGGING
        self.log('Aggregating Observation from MAF')
        ###############

        obs_df = ObservationBuilder().build(maf_df, self.index_name)
        return obs_df

    def build_case(self, maf_df):
        """
        Case
        """
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        ###############
        # LOGGING
        self.log_count(case_df)
        ###############

        return case_df
