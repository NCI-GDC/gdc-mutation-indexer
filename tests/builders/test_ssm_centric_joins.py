import pytest
import json
from pyspark.sql.functions import explode, lit, col

from exports.builders.utils import uuid5_col
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'ssm_centric_df', 'ssm_transcript_df')
class TestSSMCentricJoins:
    """
        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
    """

    def test_consequences_per_ssm(self, maf_df, ssm_centric_df, ssm_transcript_df):

        # Consequences per SSM built:
        df = (ssm_centric_df.select('ssm_id',
                                    explode('consequence').alias('consequence'))
                            .select('ssm_id', 'consequence.consequence_id'))
        es_cps = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_cps.setdefault(row['ssm_id'], set())
            es_cps[row['ssm_id']].update({row['consequence_id']})

        # Consequences per SSM expected:
        # Consequence ~ UUID[ssm_id, transcript_id]
        cps = {}
        df = (ssm_transcript_df.withColumn('consequence_id',
                                           uuid5_col(lit('ssm_consequence'),
                                                     col('ssm_id'),
                                                     col('transcript_id'))))

        for row in df.toJSON().collect():
            row = json.loads(row)
            cps.setdefault(row['ssm_id'], set())
            cps[row['ssm_id']].update({row['consequence_id']})

        assert es_cps == cps

    def test_occurrences_per_ssm(self, maf_df, ssm_centric_df):
        # Occurrences per SSM built:
        df = (ssm_centric_df.select('ssm_id',
                                    explode('occurrence').alias('occurrence'))
                            .select('ssm_id', 'occurrence.occurrence_id'))
        es_ops = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_ops.setdefault(row['ssm_id'], set())
            es_ops[row['ssm_id']].update({row['occurrence_id']})

        # Occurrences per SSM expected:
        ops = {}
        df = maf_df.select('ssm_id', 'occurrence_id').distinct()
        for row in df.toJSON().collect():
            row = json.loads(row)
            ops.setdefault(row['ssm_id'], set())
            ops[row['ssm_id']].update({row['occurrence_id']})

        assert es_ops == ops
