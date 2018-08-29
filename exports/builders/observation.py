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

    def build(self, maf_df, index):
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

    def build_for_cnv(self, initial_df):
        """
        observation[]
        |____ observation{}
                |____ observation_id
                |____ variant_status
                |____ variant_calling {}
                        |____ variant_caller

        """
        # TODO: this should maybe move to cnv_centric
        if 'occurrence_id' not in initial_df.columns:
            # add occurrence id to map to higher level occurrence
            new_df = initial_df.withColumn('occurrence_id',
                                           uuid5_col(col('cnv_id'),
                                                     col('case_id')))

        # add observation id
        new_df = new_df.withColumn('observation_id',
                                   uuid5_col(col('cnv_id'),
                                             col('case_id'),
                                             col('aliquot_id')))

        # add other observation fields
        new_df = new_df.withColumn('variant_status', lit('Tumor only'))
        new_df = new_df.withColumn('variant_caller', lit('GISTIC2'))
        new_df = new_df.withColumn('variant_calling', struct('variant_caller')
                                   .alias('variant_calling'))
        new_df = new_df.drop('variant_caller')

        # observation structure, TODO: add more fields
        obs_df = (new_df.select('cnv_id',
                                'case_id',
                                'occurrence_id',
                                struct('observation_id',
                                       'variant_status',
                                       'variant_calling').alias('observation'))
                        .groupby('cnv_id', 'case_id', 'occurrence_id')
                        .agg(collect_set('observation').alias('observation')))

        return obs_df
