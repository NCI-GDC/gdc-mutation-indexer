import pytest
import json

from pyspark.sql.functions import col, lit

from exports.builders.consequence import ConsequenceBuilder
from exports.builders.utils import uuid5_col
from tests_config import TestConfig
from base_joins_test import BaseJoinsTest

conf = TestConfig()


@pytest.mark.usefixtures(
    'maf_df', 'ssm_transcript_df',
    'ssm_occurrence_centric_df', 'ssm_occurrence_ssm_subtree'
)
class TestSSMOccurrenceCentricJoins(BaseJoinsTest):
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

    def test_consequences_per_ssm_occurrence(
            self, ssm_transcript_df, ssm_occurrence_centric_df):

        # Consequences per SSM Occurrence built:
        df = ssm_occurrence_centric_df.select('ssm_occurrence_id',
                                              'ssm.ssm_id')
        ssm_occ_to_ssm = self.get_relationship_map(
            df, 'ssm_occurrence_id', 'ssm_id'
        )

        # SSM to SSM Occurrence expected:
        df = (ssm_transcript_df
                  .withColumn('consequence_id',
                              uuid5_col(lit('ssm_consequence'),
                                        col('ssm_id'),
                                        col('transcript_id')))
                  .withColumn('ssm_occurrence_id', col('occurrence_id'))
                  .select('ssm_occurrence_id', 'ssm_id', 'consequence_id',
                          'transcript_id', 'gene_id'))

        true_ssm_occ_to_ssm = self.get_relationship_map(
            df, 'ssm_occurrence_id', 'ssm_id'
        )

        assert ssm_occ_to_ssm == true_ssm_occ_to_ssm

    @pytest.mark.xfail(reason="'observation_id' is not part of MAF DF anymore")
    def test_observations_per_ssm_occurrence(
            self, maf_df, ssm_occurrence_centric_df):

        # Observations and Cases per SSM Occurrence built:
        df = self.unpack_df_list(ssm_occurrence_centric_df,
                                 ['ssm_occurrence_id', 'case.case_id'],
                                 'case.observation', 'observation_id')

        opo = self.get_relationship_map(df, 'ssm_occurrence_id', 'observation_id')
        cpo = self.get_relationship_map(df, 'ssm_occurrence_id', 'case_id')

        # Observations and Cases per SSM Occurrence expected:
        df = (maf_df.withColumn('ssm_occurrence_id', col('occurrence_id'))
                    .select('ssm_occurrence_id', 'variant_caller', 'case_id'))

        true_opo = self.get_relationship_map(df, 'ssm_occurrence_id', 'observation_id')
        true_cpo = self.get_relationship_map(df, 'ssm_occurrence_id', 'case_id')

        assert opo == true_opo
        assert cpo == true_cpo

    @pytest.mark.ssm_occurrence_centric_ssm_subtree
    def test_ssm_subtree(self, sqlContext, maf_df, ssm_occurrence_ssm_subtree):
        def get_stats(dataframe):
            """
            Extracts ssm, consequence, transcript, gene relationships
            from a flat dataframe
            """
            res = {}
            for row in dataframe.toJSON().collect():
                row = json.loads(row)
                sid = row['ssm_id']
                cid = row['consequence_id']
                tid = row['transcript_id']
                gid = row['gene_id']

                res.setdefault(sid, {})
                res[sid].setdefault(
                    cid, {'transcripts': set(), 'genes': set()}
                )
                res[sid][cid]['transcripts'].update([tid])
                res[sid][cid]['genes'].update([gid])

            return res

        fields_to_unpack = [
            'consequence_id',
            'transcript.transcript_id',
            'transcript.gene.gene_id'
        ]

        # ssm_subtree stats expected:
        cons_df = (
            ConsequenceBuilder(conf, sqlContext).build_for_ssm(
                maf_df, 'ssm_occurrence_centric',
                join_gene=True
            )
        )
        df = self.unpack_df_list(cons_df,
                                 'ssm_id', 'consequence',
                                 fields_to_unpack)
        true_stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(ssm_occurrence_ssm_subtree,
                                 'ssm_id', 'ssm.consequence',
                                 fields_to_unpack)
        stats = get_stats(df)

        assert stats == true_stats

