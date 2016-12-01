from utils import SparkTestCase

import pytest
from conftest import ES_INDEX

from exports.aggregator import Aggregator


class TestAggregator(SparkTestCase):


    def test_patch_url(self):
        ''' Test that s3 urls are patched correctly '''
        agg = Aggregator()
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        self.assertTrue(agg.patch_url(url1).startswith('s3a://'))
