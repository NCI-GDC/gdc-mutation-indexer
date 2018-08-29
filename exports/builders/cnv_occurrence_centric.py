import logging

from pyspark.sql.functions import (
    col,
    struct,
)

from exports.builders import (
    BaseBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    GisticBuilder,
    ObservationBuilder,
)

logging.basicConfig()


class CNVOccurrenceCentricBuilder(BaseBuilder):
    """
    Builds cnv-occurrence-centric dataframe given
    case, gene, and maf dataframes:

        cnv_occurrence{}
        |____ case{}
        |       |____ observation[]
        |____ cnv{}
                |____ consequence[]
                            |_____ gene{}
    """

    index_name = 'cnv_occurrence_centric'
    id_field = 'cnv_occurrence_id'

    def build(self, maf_df):
        """
        Builds CNV Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_occurrence_centric = self.get_existing()
            if self.cnv_occurrence_centric is not None:
                return self

        cnv_cons = self.build_cnv_subtree(maf_df)

        case_obs_df = self.build_case_subtree(maf_df, cnv_cons)

        self.log('Joining cnv with case')
        cnv_occurrence_centric = (cnv_cons.join(case_obs_df,
                                                on=['case_id', 'cnv_id'],
                                                how='inner')
                                          .withColumn('cnv_occurrence_id',
                                                      col('occurrence_id'))
                                          .drop('case_id')
                                          .drop('cnv_id')
                                          .drop('occurrence_id'))
        self.log_count(cnv_occurrence_centric)

        self.cnv_occurrence_centric = cnv_occurrence_centric
        self.log('Build finished')
        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_cnv_subtree(self, maf_df):
        """
            cnv{}
                |____ consequence[]
                            |_____ gene{}
        """

        # CNV
        # read from gistic
        cnv_df = GisticBuilder(self.config, self.sqlContext).build(maf_df)
        self.log_count(cnv_df)

        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build_for_cnv(cnv_df))

        self.log('Final join CNV + Consequence')
        cnv_cons = cnv_df.join(cons_df, on='cnv_id', how='right') # TODO: structure is right?
        self.log_count(cnv_cons)

        return cnv_cons

    def build_case_subtree(self, maf_df, cnv_df):
        """
            case{}
        |       |____ observation[]
        """
        self.log('Building case subtree')
        # Observation
        obs_df = ObservationBuilder().build_for_cnv(cnv_df)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        self.log_count(case_df)

        self.log('Join observation with case')
        case_obs_df = (case_df.join(obs_df, on=['case_id'], how='right')
                              .select('case_id', 'cnv_id', 'occurrence_id',
                                      struct('observation',
                                             *case_df.columns)
                                      .alias('case')))
        self.log_count(case_obs_df)
        return case_obs_df
