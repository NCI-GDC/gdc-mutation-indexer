import logging

from pyspark.sql.functions import lit, struct, collect_list, col

from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.mappers import ModelMapper
from exports.builders.utils import uuid5_col
from exports.builders.df_builders import (
    get_ssm_df
)
logging.basicConfig()

logging.basicConfig()


class SSMCentricBuilder(BaseBuilder):
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
    mapper = ModelMapper('ssm_centric')

    def build(self, maf_df):
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.ssm_centric = self.get_existing()
            if self.ssm_centric is not None:
                return self

        ssm_df = get_ssm_df(maf_df, unique_fields=['ssm_id'])

        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(maf_df, join_gene=True))

        occurrence_df = self.build_occurrence(maf_df)

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
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_occurrence(self, maf_df):
        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Joining Cases with Observation, [right, case_id]')
        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .withColumn('ssm_occurrence_id', col('occurrence_id'))
                         .select('ssm_id',
                                 struct('occurrence_id', 'ssm_occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('ssm_id')
                         .agg(collect_list('occurrence').alias('occurrence')))
        self.log_count(occurrence_df)
        return occurrence_df
