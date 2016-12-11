import os
import json
import pytest
from collections import Counter

from utils import SparkTestCase
from exports.builders import MAFBuilder
from config import TestConfig


class TestMAFBuilder(SparkTestCase):

    def test_patch_url(self):
        ''' Test that s3 urls are patched correctly '''
        builder= MAFBuilder(TestConfig, self.sqlContext)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        self.assertTrue(builder.patch_url(url1).startswith('s3a://'))

    def test_builder(self):
        builder= MAFBuilder(TestConfig, self.sqlContext)
        urls = ['file://'+os.path.join(TestConfig.data_dir, 'test.mutect.maf'),
                'file://'+os.path.join(TestConfig.data_dir, 'test.muse.maf')]


        df = builder.combine(urls)
        self.assertEqual(df.count(), 48)
        c = Counter([json.loads(item)['variant_caller'] for item
                     in df.select('variant_caller').toJSON().collect()])
        self.assertEqual(c['mutect'], 30)
        self.assertEqual(c['muse'], 18)
