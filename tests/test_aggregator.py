from utils import SparkTestCase

import pytest

from exports.aggregator import Aggregator
from config import TestConfig


class TestAggregator(SparkTestCase):

    def test_patch_url(self):
        ''' Test that s3 urls are patched correctly '''
        agg = Aggregator(TestConfig)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        self.assertTrue(agg.patch_url(url1).startswith('s3a://'))
