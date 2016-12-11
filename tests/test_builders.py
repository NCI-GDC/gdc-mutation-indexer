import os
import json
import pytest
import unittest
from collections import Counter

from utils import SparkTestCase
from exports.builders import MAFBuilder
from exports.builders.utils import ssm_uuid, ssm_label
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

class TestBuilderUtils(unittest.TestCase):
    
    def test_ssm_label(self):
        ''' Test ssm label generation '''
        label = ssm_label('chr3','SNP',41589825,'','A','T')
        self.assertEqual(label, '3:g.41589825A>T')

        label = ssm_label('chr3','DEL',41589825,'','A','')
        self.assertEqual(label, '3:g.41589825delA')

        label = ssm_label('chr3','INS',41589825,41589825,'','T')
        self.assertEqual(label, '3:g.41589825_41589825insT')

    def test_ssm_uuid(self):
        ''' Test ssm_id generation '''
        ssm_id = ssm_uuid(TestConfig.ssm_namespace,'chr3','SNP',41589825,'','A','T')
        self.assertEqual(ssm_id, '23f64415-9f13-5854-8450-b6ab83a9911b')
