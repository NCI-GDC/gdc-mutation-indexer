import os
import json

from tests_config import TestConfig
from utils import SparkTestCase

from pyspark.sql.functions import size, sum

from exports.builders import ObservationBuilder, MAFBuilder
conf = TestConfig()


class TestObservationBuilder(SparkTestCase):
    ''' Test intermediate result from the observation builder '''

    @classmethod
    def setUpClass(cls):
        super(TestObservationBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()

    def test_join_columns(self):
        ''' Check for columns that are used by other builders to join on '''
        obs_df = ObservationBuilder(conf, self.sqlContext)\
                                   .build(self.maf_df)
        self.assertIn('_case_submitter_id', obs_df.columns)

    def test_observation_size(self):
        ''' Check for the right number of observations by submitter_id '''
        obs_df = ObservationBuilder(conf, self.sqlContext).build(self.maf_df)
        self.assertEqual(obs_df.count(), 18)

    def test_observation_values(self):
        obs_df = ObservationBuilder(conf, self.sqlContext).build(self.maf_df)

        # There are three observations for TCGA-KL-8328
        case_row = obs_df.where(obs_df._case_submitter_id == 'TCGA-KL-8328')
        self.assertEqual(case_row.count(), 3)


        case_row = obs_df.where(obs_df._case_submitter_id == 'TCGA-B2-4102')
        self.assertEqual(case_row.count(), 1)
        obs = json.loads(case_row.toJSON().collect()[0])
        self.assertEqual(obs['ssm_id'], '1f18a8c6-d828-5a64-b26a-4e1c7a1734db')
        self.assertEqual(len(obs['observation']), 1)
        obs = obs['observation'][0]
        self.assertDictEqual(obs[u'tumor_genotype'], {
                                   "tumor_seq_allele1": "T",
                                   "tumor_seq_allele2": "G"
                              })

        self.assertDictEqual(obs['read_depth'], {
                                   "t_depth": 162,
                                   "t_alt_count": 41,
                                   "t_ref_count": 121,
                                   "n_depth": 162
                              })

        self.assertDictEqual(obs['variant_calling'], {
                                    'variant_process': 'masked',
                                    'variant_caller': 'mutect2'
                              })
