import logging

from pyspark.sql.functions import col, struct

from exports.builders.df_builders import build_ssm_subtree
from exports.builders import (
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder

logging.basicConfig()


class SSMOccurrenceCentricBuilder(BaseBuilder):
    """
    Builds ssm-occurrence-centric dataframe given case and maf dataframes::

        ssm_occurrence{}
              |____ ssm{}
              |        |____ consequence[]
              |                     |_____ transcript{}
              |                                   |_____ gene{}
              |                                   |_____ annotation{}
              |____ case{}
                       |____ observation[]
    """

    index_name = 'ssm_occurrence_centric'
    id_field = 'ssm_occurrence_id'

    def build(self, maf_df):
        """
        Builds SSM Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.ssm_occurrence_centric = self.get_existing()
            if self.ssm_occurrence_centric is not None:
                return self

        case_obs_df = self.build_case_subtree(maf_df)

        ssm_cons = self.build_ssm_subtree(maf_df)

        self.log('Joining ssm with case')
        ssm_occurrence_centric = (ssm_cons.join(case_obs_df,
                                                on=['case_id', 'ssm_id'],
                                                how='inner')
                                          .withColumn('ssm_occurrence_id',
                                                      col('occurrence_id'))
                                          .drop('case_id')
                                          .drop('ssm_id')
                                          .drop('occurrence_id'))
        self.log_count(ssm_occurrence_centric)

        self.ssm_occurrence_centric = ssm_occurrence_centric
        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_ssm_subtree(self, maf_df):
        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(maf_df, self.index_name, join_gene=True))

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, self.index_name).drop('gene_id')
        self.log_count(ssm_df)

        ssm_cons = ssm_df.select('ssm_id', 'case_id',
                                 struct('consequence',
                                        *ssm_df.drop('consequence')
                                               .drop('case_id').columns)
                                 .alias('ssm'))
        self.log_count(ssm_cons)

        return ssm_cons

    def build_case_subtree(self, maf_df):
        self.log('Building case subtree')
        # Observation
        obs_df = ObservationBuilder().build(maf_df, self.index_name)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        self.log_count(case_df)

        self.log('Join observation with case')
        case_obs_df = (case_df.join(obs_df, on=['case_id'], how='right')
                              .select('case_id', 'ssm_id', 'occurrence_id',
                                  struct(
                                      'observation',
                                      *case_df.columns
                                  ).alias('case')))
        self.log_count(case_obs_df)
        return case_obs_df

