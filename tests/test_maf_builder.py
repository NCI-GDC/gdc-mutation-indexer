import os
import re
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

    def test_mutation_type(self):
        '''
        Test that mutation_type is created properly
        '''
        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        self.assertIn('mutation_type', df.columns)

        # Should only have 1 type, 'Simple Somatic Mutation'
        self.assertEqual(df.select('mutation_type').distinct().count(), 1)
        self.assertEqual(df.select('mutation_type').collect()[0]['mutation_type'],
                         'Simple Somatic Mutation')


    def test_variant_caller(self):
        '''
        Test that variant caller is created properly
        '''
        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        self.assertIn('variant_caller', df.columns)

        muse = df.where(df.variant_caller == 'muse')
        self.assertEqual(muse.count(), 7)
        mutect = df.where(df.variant_caller == 'mutect')
        self.assertEqual(mutect.count(), 11)


    def test_variant_process(self):
        '''
        Test that variant process is created properly
        '''
        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        self.assertIn('variant_process', df.columns)
        self.assertEqual(df.first()['variant_process'], 'masked')


    def test_mutation_subtype(self):
        '''
        Test that mutation_subtype is created properly
        '''
        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        self.assertIn('mutation_subtype', df.columns)

        # Should have as many distinct variants as subtypes
        self.assertEqual(df.select('variant_type').distinct().count(),
                         df.select('mutation_subtype').distinct().count())

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

    def test_maf_field_types(self):
        """
        Test that maf_df field types correspond to maf.yml
        """
        builder = MAFBuilder(TestConfig(), self.sqlContext)
        df = builder.build()

        types = {'int': 'integer', 'bool': 'boolean', 'float': 'float'}
        for col in df.schema:
            col_info = json.loads(col.json())
            if col_info['name'] in builder.schema:
                if 'type' in builder.schema[col_info['name']]:
                    assert col_info['type'] == types[builder.schema[col_info['name']]['type']]

    def test_maf_field_pattern(self):
        """
        Test that maf_df field pattern correspond to maf.yml
        """
        builder = MAFBuilder(TestConfig(), self.sqlContext)
        df = builder.build()

        for col in df.schema:
            col_info = json.loads(col.json())
            colname = col_info['name']
            if colname in builder.schema:
                if 'pattern' in builder.schema[colname]:
                    pattern = builder.schema[colname]['pattern']
                    values = df.select(colname)
                    for row in values.collect():
                        val = row[colname]
                        is_matching = re.search(pattern.replace('{}','.*'), val)
                        assert is_matching

