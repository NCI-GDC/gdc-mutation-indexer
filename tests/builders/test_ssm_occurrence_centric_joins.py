import pytest
import json

from pyspark.sql.functions import explode, col, lit

from exports.builders.utils import uuid5_col
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'ssm_occurrence_centric_df', 'ssm_transcript_df')
class TestSSMOccurrenceCentricJoins:
    """
        ssm_occurrence{}
              |____ ssm{}
              |        |____ consequence[]
              |                     |_____ transcript{}
              |                                   |_____ gene{}
              |                                   |_____ annotation{}
              |____ case{}
                       |____ observation[]
    """

    def test_consequences_per_ssm_occurrence(self, maf_df, ssm_transcript_df,
                                             ssm_occurrence_centric_df):
        # Consequences per SSM Occurrence built:
        df = (ssm_occurrence_centric_df.select('ssm_occurrence_id',
                                               'ssm.ssm_id',
                                               explode('ssm.consequence')
                                               .alias('consequence'))
                                       .select('ssm_occurrence_id', 'ssm_id',
                                               'consequence.consequence_id'))
        es_cpo = {}
        es_ssm_occ_to_ssm = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_cpo.setdefault(row['ssm_occurrence_id'], set([]))
            es_cpo[row['ssm_occurrence_id']].update({row['consequence_id']})
            # Also save ssm_occurrence_id <-> ssm_id pairs to compare:
            es_ssm_occ_to_ssm[row['ssm_occurrence_id']] = row['ssm_id']

        # Consequences per SSM Occurrence expected:
        cpo = {}
        ssm_occ_to_ssm = {}
        df = (ssm_transcript_df.withColumn('consequence_id',
                                           uuid5_col(lit('ssm_consequence'),
                                                     col('ssm_id'),
                                                     col('transcript_id')))
                               .withColumn('ssm_occurrence_id', col('occurrence_id'))
                               .select('ssm_id', 'ssm_occurrence_id', 'consequence_id'))

        for row in df.toJSON().collect():
            row = json.loads(row)
            cpo.setdefault(row['ssm_occurrence_id'], set([]))
            cpo[row['ssm_occurrence_id']].update({row['consequence_id']})
            # Also save ssm_occurrence_id <-> ssm_id pairs to compare:
            ssm_occ_to_ssm[row['ssm_occurrence_id']] = row['ssm_id']

        assert es_cpo == cpo
        assert es_ssm_occ_to_ssm == ssm_occ_to_ssm

    def test_observations_per_ssm_occurrence(self, maf_df, ssm_occurrence_centric_df):
        # Observations per SSM Occurrence built:
        df = (ssm_occurrence_centric_df.select('ssm_occurrence_id',
                                               'case.case_id',
                                               explode('case.observation')
                                               .alias('observation'))
                                       .select('ssm_occurrence_id', 'case_id',
                                               'observation.observation_id'))
        es_opo = {}
        es_ssm_occ_to_case = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_opo.setdefault(row['ssm_occurrence_id'], set([]))
            es_opo[row['ssm_occurrence_id']].update({row['observation_id']})
            # Also save ssm_occurrence_id <-> case_id pairs to compare:
            es_ssm_occ_to_case[row['ssm_occurrence_id']] = row['case_id']

        # Observations per SSM Occurrence expected:
        opo = {}
        ssm_occ_to_case = {}
        df = (maf_df.withColumn('ssm_occurrence_id', col('occurrence_id'))
                    .select('ssm_occurrence_id', 'observation_id', 'case_id'))

        for row in df.toJSON().collect():
            row = json.loads(row)
            opo.setdefault(row['ssm_occurrence_id'], set([]))
            opo[row['ssm_occurrence_id']].update({row['observation_id']})
            # Also save ssm_occurrence_id <-> case_id pairs to compare:
            ssm_occ_to_case[row['ssm_occurrence_id']] = row['case_id']

        assert es_opo == opo
        assert es_ssm_occ_to_case == ssm_occ_to_case
