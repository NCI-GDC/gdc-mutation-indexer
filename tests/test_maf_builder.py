import os
import json
import yaml
import pytest
import unittest
from collections import Counter

from utils import SparkTestCase
from exports.builders import MAFBuilder
from exports.builders.utils import ssm_label
from config import TestConfig


class TestMAFBuilder(SparkTestCase):

    def test_patch_url(self):
        ''' Test that s3 urls are patched correctly '''
        builder = MAFBuilder(TestConfig(), self.sqlContext)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        self.assertTrue(builder.patch_url(url1).startswith('s3a://'))

    def test_combine(self):
        '''
        Test that mafs are combined correctly
        '''
        builder= MAFBuilder(TestConfig(), self.sqlContext)

        df = builder.combine(TestConfig().maf_urls)
        self.assertEqual(df.count(), 18)
        c = Counter([json.loads(item)['variant_caller'] for item
                     in df.select('variant_caller').toJSON().collect()])
        self.assertEqual(c['mutect'], 11)
        self.assertEqual(c['muse'], 7)

    def test_schema(self):
        '''
        Test that maf has columns correctly renamed
        '''
        builder = MAFBuilder(TestConfig(), self.sqlContext)

        df = builder.combine(TestConfig().maf_urls)
        df = builder.standardize_schema(df)

        path = os.path.join(os.path.dirname(__file__), '../exports/schemas/maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']

        for field in maf_schema.keys():
            self.assertIn(field, df.columns)

    def test_ssm_id(self):
        '''
        Test that ssm_id column is created
        '''
        builder = MAFBuilder(TestConfig(), self.sqlContext)

        df = builder.combine(TestConfig().maf_urls)
        df = builder.standardize_schema(df)
        df = builder.add_ssm_id(df)
        self.assertIn('ssm_id', df.columns)

    def test_genomic_dna_change(self):
        '''
        Test that the genomic_dna_change is created correctly
        '''
        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        self.assertIn('genomic_dna_change', df.columns)

        # Number of unique labels should be equal to the number of unique ssm
        self.assertEqual(df.select('ssm_id').distinct().count(),
                         df.select('genomic_dna_change').distinct().count())

        labels = [r['genomic_dna_change'] for r
                  in df.select('genomic_dna_change').collect()]

        self.assertIn('chr1:g.32180498T>C', labels)
        self.assertIn('chr2:g.182729892G>T', labels)
        self.assertIn('chr9:g.2056812T>A', labels)

    def test_case_barcode(self):
        '''
        Test that ssm_id column is created
        '''
        builder = MAFBuilder(TestConfig(), self.sqlContext)

        df = builder.combine(TestConfig().maf_urls)
        df = builder.standardize_schema(df)
        df = builder.extract_barcode(df)

        self.assertIn('_case_submitter_id', df.columns)
        self.assertEqual(df.where(df.tumor_sample_barcode=='TCGA-A4-A6HP-01A-11D-A31X-10')\
                           .select('_case_submitter_id').limit(1).collect()[0]._case_submitter_id,
                           'TCGA-A4-A6HP')
