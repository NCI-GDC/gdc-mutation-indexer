import logging

from exports.builders import (
    ObservationBuilder
)
from exports.builders import BaseBuilder

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CaseForCNVJoinsCentricBuilder(BaseBuilder):
    """
    Builds case docs for joining to CNV.

        case{}
            |____ observation[]
    """

    index_name = 'case_for_cnv_joins_centric'
    id_field = 'case_id'

    def build(self, maf_df, case_df):
        """
        TODO
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.case_for_cnv_joins_centric = self.get_existing()
            if self.case_for_cnv_joins_centric is not None:
                return self

        self.case_for_cnv_joins_centric = \
            self.build_case_for_cnv(maf_df, case_df)

        self.log('Build finished')

        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    def build_case_for_cnv(self, maf_df, case_df):

        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder().build_for_cnv(gistic_df, self.index_name)
        import ipdb; ipdb.set_trace()

        self.log('Joining Cases with Observation, [right, case_id]')
        case_for_cnv_df = (case_df.join(obs_df,
                                        on=['case_id'],
                                        how='right')
                           ).drop('cnv_id').drop('occurrence_id')

        self.log_count(case_for_cnv_df)

        return case_for_cnv_df
