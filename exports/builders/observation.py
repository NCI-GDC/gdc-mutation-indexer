import logging

from pyspark.sql.functions import (
    struct, collect_list, collect_set,
    lit, col,
)

from exports.builders.utils import struct_select, uuid5_col

logging.basicConfig()


class ObservationBuilder(object):
    """
    Builds observation dataframe from the maf dataframe
    """

    def build_for_ssm(self, maf_df, index):
        """
        Builds an observation from a maf.
        Each line of a maf is roughly an observation, though it could be better
        said that a unique observation is identified by a unqiue pairing of
        tumor and normal sample uuids and an ssm uuid.
        """

        obs_df = (maf_df.select('ssm_id', 'case_id', 'occurrence_id',
                                struct(*struct_select(index,
                                                      'observation'))
                                .alias('observation'))
                        .groupby('ssm_id', 'case_id', 'occurrence_id')
                        .agg(collect_list('observation')
                             .alias('observation')))

        return obs_df

    def build_for_cnv(self, gistic_df, index):
        """
        observation[]
        |____ observation{}
                |____ observation_id
                |____ variant_status
                |____ variant_calling {}
                        |____ variant_caller

        """

        # add other observation fields
        obs_df = gistic_df.withColumn('variant_caller', lit('GISTIC2'))
        obs_df = obs_df.withColumn('variant_calling', struct('variant_caller')
                                   .alias('variant_calling'))
        obs_df = obs_df.drop('variant_caller')

        # observation structure, TODO: add more fields
        # TODO: use struct_select(index, 'observation')!!!
        obs_df = (obs_df.select('cnv_id',
                                'case_id',
                                'occurrence_id',
                                struct('observation_id',
                                       'variant_status',
                                       'variant_calling').alias('observation'))
                        .drop('variant_status')  # ?
                        .groupby('cnv_id', 'case_id', 'occurrence_id')
                        .agg(collect_set('observation').alias('observation')))

        return obs_df
