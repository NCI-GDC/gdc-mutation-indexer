import logging

from pyspark.sql import SQLContext
from pyspark.sql.functions import struct, collect_list

from exports import builders
from exports.builders.df_builders import (
    get_ssm_df
)

from config import BaseConfig, LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class SSMCentricBuilder(builders.BaseBuilder):
    """
    Builds ssm-centric dataframe given case and maf dataframes::

        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
    """

    index_name = 'ssm_centric'
    id_field = 'ssm_id'

    def __init__(
        self,
        config: BaseConfig,
        sqlContext: SQLContext,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(self, maf_df, case_df):
        """
        Builds SSM Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == 'read':
            self.ssm_centric = self.load_raw()
            if self.ssm_centric is not None:
                return self

        ssm_df = get_ssm_df(maf_df, self.index_name, unique_fields=['ssm_id'])

        cons_df = self.build_consequence(maf_df)

        occurrence_df = self.build_occurrence(maf_df, case_df)

        self.log('Final join SSM + Consequence + Occurrence')
        ssm_centric = ssm_df.join(cons_df, on='ssm_id')\
                            .join(occurrence_df, on='ssm_id')

        # Truncate outliers
        treshold = self.config.percentile_threshold['occurrences_per_ssm']
        self.ssm_centric = self.truncate_df_at_percentile(ssm_centric,
                                                          'occurrence',
                                                          treshold)
        self.log_count(self.ssm_centric)
        self.log('Build finished')

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_consequence(self, maf_df):
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df,
            self.index_name,
            join_gene=True,
            add_gene_aa_change=True
        )

        return cons_df

    def build_occurrence(self, maf_df, case_df):
        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = self.observation_builder.build_for_ssm(
            maf_df,
            self.index_name,
        )

        self.log('Joining Cases with Observation, [right, case_id]')
        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('ssm_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('ssm_id')
                         .agg(collect_list('occurrence').alias('occurrence')))
        self.log_count(occurrence_df)

        return occurrence_df
