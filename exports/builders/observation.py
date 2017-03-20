import logging
logging.basicConfig()

from pyspark.sql.functions import struct, collect_list

from exports.builders import BaseBuilder
from exports.builders.utils import struct_select


class ObservationBuilder(BaseBuilder):
    '''
    Builds observation dataframe from the maf dataframe
    '''

    def build(self, maf_df):
        '''
        Builds an observation from a maf.
        Each line of a maf is roughly an observation, though it could be better
        said that a unique observation is identified by a unqiue pairing of
        tumor and normal sample uuids and an ssm uuid.
        '''

        obs_df = (maf_df.select('ssm_id', 'case_id',
                                'occurrence_id', 'observation_id',
                                struct(*struct_select('observation.yml'))
                                .alias('observation'))
                        .groupby('ssm_id', 'case_id', 'occurrence_id')
                        .agg(collect_list('observation')
                             .alias('observation')))

        return obs_df
