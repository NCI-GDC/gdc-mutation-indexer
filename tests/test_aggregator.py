import os
import json
import pytest
from collections import Counter


from utils import SparkTestCase
from exports.aggregator import Aggregator
from config import TestConfig


class TestAggregator(SparkTestCase):

    def test_patch_url(self):
        ''' Test that s3 urls are patched correctly '''
        agg = Aggregator(TestConfig, self.sqlContext)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        self.assertTrue(agg.patch_url(url1).startswith('s3a://'))

    def test_aggregated(self):
        agg = Aggregator(TestConfig, self.sqlContext)
        urls = ['file://'+os.path.join(TestConfig.data_dir, 'test.mutect.maf'),
                'file://'+os.path.join(TestConfig.data_dir, 'test.muse.maf')]


        df = agg.combine(urls)
        self.assertEqual(df.count(), 48)
        c = Counter([json.loads(item)['variant_caller'] for item
                     in df.select('variant_caller').toJSON().collect()])
        self.assertEqual(c['mutect'], 30)
        self.assertEqual(c['muse'], 18)
