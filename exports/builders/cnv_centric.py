import logging

from pyspark.sql.functions import struct, collect_list

from exports.builders import (
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder

logging.basicConfig()


class CNVCentricBuilder(BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case and maf dataframes::

        cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                    |_____ case{}
                                |____ observation[]

    TODO: this is kinda like mafBuilder, but also kinda like the index_builders.
    figure out what to do about that
    """

    # region Class-level Fields
    index_name = 'cnv_centric'
    id_field = 'cnv_id'
    # endregion

    # region Constructor

    def __init__(self, config, sqlContext):
        BaseBuilder.__init__(self, config, sqlContext)
        self._url = self.get_url(config)

    # endregion

    # region Abstract Overrides

    def build(self, maf_df):
        """
        Builds CNV Centric index

        # TODO: Load from gistic files
        # "real" file is in BRCA / all_thresholded.by_genes.txt
        # fake file is (-1).txt
        # gonna read it in a la maf.py
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric = self.get_existing()
            if self.cnv_centric is not None:
                return self

        # read from gistic
        initial_cnv_df = self.get_initial_cnv()

        # do joins
        # joined_cnv_df = self.join_cnv(initial_cnv_df, maf_df)

        # truncate outliers
        # cnv_centric_df = self.truncate(joined_cnv_df)

        cnv_centric_df = initial_cnv_df

        # save final df as property
        self.cnv_centric = cnv_centric_df

        ###############
        # LOGGING
        self.log_count(self.cnv_centric)
        self.log('Build finished')
        ###############

        # Check if we should write
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    # endregion

    # region Private Helper Functions

    def truncate(self, cnv_df_to_truncate):
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        truncated_df = self.truncate_df_at_percentile(cnv_df_to_truncate,
                                                      'occurrence',
                                                      threshold)

        return truncated_df

    def join_cnv(self, cnv_df, maf_df):

        cons_df = self.build_consequence(maf_df)
        occurrence_df = self.build_occurrence(maf_df)

        ##############
        # LOGGING
        self.log('Final join CNV + Consequence + Occurrence')
        ###############

        cnv_centric_df = cnv_df.join(cons_df, on='cnv_id')\
                               .join(occurrence_df, on='cnv_id')

        return cnv_centric_df

    def get_url(self, config):
        if config.gistic_url is not None:
            return config.gistic_url

        # TODO: what should this actually be?
        return "stuff from indexd most likely"

    def get_initial_cnv(self, url=None):
        """
        Inevitably there's crap in this gistic
        """

        if url is None:
            if self._url is not None:
                url = self._url
            else:
                self.logger.error("Url not passed, instance _urls not set")
                raise Exception("Url not specified to load CNV")

        # to return
        return_df = None

        try:
            new_df = self.read_gistic(url)

            # do some crap

            self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                           url))

            return_df = new_df
        except BaseException as e:
            self.logger.error(e)
            # TODO: reraise?

        assert return_df is not None
        # TODO: ?? self.df = return_df

        return return_df

    def read_gistic(self, url=None):
        """
        Reads in dataframes from a url?
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def build_consequence(self, maf_df):
        """
        I have no idea what these options mean...
        join_gene, add_gene_aa_change?
        TODO: this is going to need to be a whole different kind
        of consequence
        """
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(input_df=maf_df,
                          index_name=self.index_name,
                          join_gene=True,
                          add_gene_aa_change=True))
        return cons_df

    def build_occurrence(self, maf_df):
        """
        Occurrence
        """
        # 1. Observation
        obs_df = self.build_observation(maf_df)

        # 2. Case
        case_df = self.build_case(maf_df)

        # 3. Join Case to Observation
        ###############
        # LOGGING
        self.log('Joining Cases with Observation, [right, case_id]')
        ###############

        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('ssm_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('ssm_id')
                         .agg(collect_list('occurrence').alias('occurrence')))

        ###############
        # LOGGING
        self.log_count(occurrence_df)
        ###############

        return occurrence_df

    def build_observation(self, maf_df):
        """
        Observation
        """
        ###############
        # LOGGING
        self.log('Aggregating Observation from MAF')
        ###############

        obs_df = ObservationBuilder().build(maf_df, self.index_name)
        return obs_df

    def build_case(self, maf_df):
        """
        Case
        """
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        ###############
        # LOGGING
        self.log_count(case_df)
        ###############

        return case_df

    # endregion
