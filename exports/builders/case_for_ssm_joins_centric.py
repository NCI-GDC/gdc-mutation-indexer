import logging

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
    id_field = 'case_id'

    def build(self, maf_df, case_df):
        """
        TODO
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.case_for_ssm_joins_centric = self.get_existing()
            if self.case_for_ssm_joins_centric is not None:
                return self

        self.case_for_ssm_joins_centric = \
            self.build_case_for_ssm(maf_df, case_df)

        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    def build_case_for_ssm(self, maf_df, case_df):

        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder().build_for_ssm(maf_df, 'ssm_centric')

        self.log('Joining Cases with Observation, [right, case_id]')
        case_for_ssm_df = (case_df.join(obs_df,
                                        on=['case_id'],
                                        how='right')
                           ).drop('ssm_id').drop('observation')

        self.log_count(case_for_ssm_df)

        return case_for_ssm_df
