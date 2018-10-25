import logging

from pyspark.sql.functions import struct, collect_list
from exports.builders import (
    ObservationBuilder
)
from exports.builders import BaseBuilder

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CaseForSSMJoinsCentricBuilder(BaseBuilder):
    """
    Builds case docs for joining to SSM.

        case{}
            |____ observation[]
    """

    index_name = 'case_for_ssm_joins_centric'
    id_field = 'case_for_ssm_joins_id'

    def build(self, maf_df, case_df):
        """
        TODO
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.ssm_centric = self.get_existing()
            if self.ssm_centric is not None:
                return self

        occurrence_df = self.build_occurrence(maf_df, case_df)

        self.case_for_ssm_joins_centric = occurrence_df
        self.log_count(self.case_for_ssm_joins_centric)

        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    def build_occurrence(self, maf_df, case_df):

        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder().build_for_ssm(maf_df, 'ssm_centric')

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
        import ipdb; ipdb.set_trace()

        occurrence_df = occurrence_df.drop('ssm_id')

        return occurrence_df
