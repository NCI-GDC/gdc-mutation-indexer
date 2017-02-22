import os
import pytest
import yaml
import json

from conftest import get_validation_paths
from config import TestConfig
from utils import SparkTestCase

from pyspark.sql.functions import size, sum

from exports.builders import ObservationBuilder, MAFBuilder
conf = TestConfig()


class TestObservationBuilder(SparkTestCase):
    ''' Test intermediate result from the observation builder '''

    def setUp(self):
        # TODO this should be setUpClass so we only build the maf once
        # Need to modify SparkTestCase to use setUpClass
        super(TestObservationBuilder, self).setUp()
        self.maf_df = MAFBuilder(conf, self.sqlContext).build()

    def test_join_columns(self):
        ''' Check for columns that are used by other builders to join on '''
        obs_df = ObservationBuilder(conf, self.sqlContext)\
                                   .build(self.maf_df)
        self.assertIn('_case_submitter_id', obs_df.columns)

    def test_observation_size(self):
        ''' Check for the right number of observations by submitter_id '''
        obs_df = ObservationBuilder(conf, self.sqlContext)\
                                   .build(self.maf_df)
        self.assertEqual(obs_df.count(), 18)
        # We should still have all 18 observations accross all cases
        self.assertEqual(obs_df.select(sum(size('observation').alias('size')))\
                            .rdd.glom().collect()[0][0][0], 18)
