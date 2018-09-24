import logging

from pyspark.sql.functions import (
    struct,
)
from exports.builders.df_builders import (
    build_cnv_subtree,
)
from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)

logging.basicConfig()


class CNVOccurrenceCentricBuilder(BaseBuilder):
    """
    Builds cnv-occurrence-centric dataframe given
    case, gene, and maf dataframes:

    cnv_occurrence{}
        |
        |____ case{}
        |       |____ observation[]
        |
        |____ cnv{}
                |____ consequence[]
                            |_____ gene{}
    """

    index_name = 'cnv_occurrence_centric'
    id_field = 'cnv_occurrence_id'

    def build(self, gistic_df, case_df):
        """
        Builds CNV Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_occurrence_centric = self.get_existing()
            if self.cnv_occurrence_centric is not None:
                return self

        self.log_count(gistic_df)

        # CNV subtree
        cnv_df = self.build_cnv_subtree(gistic_df)

        # Case subtree
        case_subtree = self.build_case_subtree(gistic_df, case_df)

        self.log('Joining cnv with case')

        cnv_occurrence_centric = (cnv_df.join(case_subtree,
                                              on=['case_id', 'cnv_id'],
                                              how='inner')
                                        .withColumnRenamed('occurrence_id',
                                                           'cnv_occurrence_id')
                                        .drop('case_id')
                                        .drop('cnv_id'))

        self.log_count(cnv_occurrence_centric)

        self.cnv_occurrence_centric = cnv_occurrence_centric
        self.log('Build finished')

        # Check if we should save the resulting dataframe
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])
        return self

    def build_cnv_subtree(self, gistic_df):
        """
            cnv{}
                |____ consequence[]
                            |_____ gene{}
        """

        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build_for_cnv(gistic_df, self.index_name))

        cnv_df = build_cnv_subtree(gistic_df,
                                   cons_df,
                                   self.index_name,
                                   obs_df=None,
                                   add_fields=['case_id'])

        cnv_subtree = cnv_df.select('cnv_id', 'case_id',
                                    struct('consequence',
                                           *cnv_df.drop('consequence')
                                                  .drop('case_id').columns)
                                    .alias('cnv'))

        return cnv_subtree

    def build_case_subtree(self, gistic_df, case_df):
        """
            case{}
                |____ observation[]
        """
        self.log('Building case subtree')

        # Observation
        obs_df = ObservationBuilder().build_for_cnv(gistic_df, self.index_name)

        self.log('Join observation with case')
        case_obs_df = (case_df.join(obs_df, on='case_id', how='left')
                              .select('case_id', 'occurrence_id', 'cnv_id',
                                      struct('observation',
                                             *case_df.columns)
                                      .alias('case')))
        self.log_count(case_obs_df)
        return case_obs_df
