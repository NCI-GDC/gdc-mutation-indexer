import logging

from pyspark.sql.functions import (
    collect_set,
    struct,
)

from exports.builders import (
    CaseBuilder,
    ObservationBuilder,
)

logging.basicConfig()


class OccurrenceBuilder(object):

    def __init__(self, config, sqlContext):
        self.logger = logging.getLogger(self.__class__.__name__)
        # TODO: complete logging
        self.config = config
        self.sqlContext = sqlContext

    def build_for_cnv(self, cnv_df, maf_df):
        """
        Assumes you've already added 'case_id'

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        assert 'case_id' in cnv_df.columns

        # 1. Observation
        self.logger.info('Aggregating Observation from gistic')
        obs_df = ObservationBuilder().build_for_cnv(cnv_df)

        # 2. Case
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)
        # self.log_count(case_df)

        # 3. Join Case to Observation and create structs
        self.logger.info('Joining Cases with Observation, [right, case_id]')
        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('cnv_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('cnv_id')
                         .agg(collect_set('occurrence').alias('occurrence')))

        # self.log_count(occurrence_df)

        return occurrence_df
