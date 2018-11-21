import logging
from pyspark.sql.functions import struct, collect_set

from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)

from exports.builders.df_builders import get_cnv_df

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CNVCentricBuilder(BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case, gene, and maf dataframes:

     cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                      |_____ case{}
                                |____ observation[]

    """

    index_name = 'cnv_centric'
    id_field = 'cnv_id'

    def build(self, gistic_df, case_df):
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == 'load':
            self.cnv_centric = self.load_raw()
            if self.cnv_centric is not None:
                return self

        self.log('Select CNV data from Gistic')
        cnv_df = get_cnv_df(gistic_df, self.index_name)

        self.log('Build Consequence')
        cons_df = (
            ConsequenceBuilder(self.config, self.sqlContext)
            .build_for_cnv(gistic_df, self.index_name)
        )

        self.log('Build Occurrence')
        occurrence_df = self.build_occurrence_df(gistic_df, case_df)

        self.log('Final join CNV + Consequence + Occurrence')
        cnv_cons_df = cnv_df.join(cons_df, on='cnv_id', how='left')
        cnv_centric_df = cnv_cons_df.join(occurrence_df, on='cnv_id',
                                          how='left')

        # truncate outliers
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        cnv_centric_df = self.truncate_df_at_percentile(cnv_centric_df,
                                                        'occurrence',
                                                        threshold)

        # save final df as property
        self.cnv_centric = cnv_centric_df

        self.log_count(self.cnv_centric)
        self.log('Build finished')

        # Check if we should write
        if self.config.output_raw == 'write':
            self.write(self.config.get_raw_output_path(self.index_name))

        return self

    def build_occurrence_df(self, gistic_df, case_df):
        """
        Assumes you've already added 'case_id'

        occurrence[]
        |____ occurrence{}
                |____ occurrence_id
                |____ case {}
                        |____ observation []

        """
        assert 'case_id' in gistic_df.columns

        # 1. Observation
        self.logger.info('Aggregating Observation from gistic')
        obs_df = ObservationBuilder().build_for_cnv(gistic_df, self.index_name)

        # 2. Join Case to Observation and create structs
        self.logger.info('Joining Cases with Observation, [right, case_id]')
        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='left')
                         .select('cnv_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('cnv_id')
                         .agg(collect_set('occurrence').alias('occurrence')))

        return occurrence_df
