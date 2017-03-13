import json
import logging

from pyspark.sql.functions import lit, struct, collect_list

from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.mappers import SSMMapper
from exports.builders.df_builders import (
    get_ssm_df
)
logging.basicConfig()

logging.basicConfig()


class SSMCentricBuilder(BaseBuilder):
    """
    Builds ssm-centric dataframe given case and maf dataframes::

        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
    """

    index_name = 'ssm_centric'
    id_field = 'ssm_id'
    mapper = SSMMapper

    def build(self, maf_df=None):
        """
        """
        if maf_df is None:
            self.log('Building MAF...')
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        ssm_df = get_ssm_df(maf_df, unique_fields=['ssm_id'])

        cons_df = ConsequenceBuilder(self.config, self.sqlContext).build(maf_df, join_gene=True)

        occurrence_df = self.build_occurrence(maf_df)

        self.log('Final join SSM + Transcript + Last one')
        ssm_centric = ssm_df.join(cons_df, on='ssm_id')\
            .join(occurrence_df, on='ssm_id')

        # Truncate outliers
        self.ssm_centric = self.truncate_df_at_percentile(ssm_centric, 'occurrence', self.config.percentile_threshold['occurrences_per_ssm'])

        self.log_count(self.ssm_centric)

        self.log('Build finished')
        return self

    def build_occurrence(self, maf_df):
        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Joining Cases with Observation, [right, submitter_id]')
        occurrence_df = case_df.join(obs_df, case_df.submitter_id == obs_df._case_submitter_id, 'right')\
                        .select('ssm_id', struct(
                            struct(
                                'observation',
                                *case_df.columns
                            ).alias('case')
                        ).alias('occurrence'))\
                        .groupby('ssm_id')\
                        .agg(collect_list('occurrence').alias('occurrence'))
        self.log_count(occurrence_df)
        return occurrence_df
