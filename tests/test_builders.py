import os
import uuid
import json
import yaml
import pytest
import unittest
from collections import Counter

from utils import SparkTestCase
from exports.builders import MAFBuilder
from exports.builders.utils import ssm_uuid, ssm_label, flat_fields, struct_select, _udf_uuid5_field
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

class TestBuilderUtils(unittest.TestCase):

    def test_ssm_label(self):
        ''' Test ssm label generation '''
        label = ssm_label('chr3','SNP',41589825,'','A','T')
        self.assertEqual(label, 'chr3:g.41589825A>T')

        label = ssm_label('chr3','DEL',41589825,'','A','')
        self.assertEqual(label, 'chr3:g.41589825delA')

        label = ssm_label('chr3','INS',41589825,41589825,'','T')
        self.assertEqual(label, 'chr3:g.41589825_41589825insT')

        label = ssm_label('chr4','SNP',112382545,'','A','T')
        self.assertEqual(label, 'chr4:g.112382545A>T')

    def test_ssm_uuid(self):
        ''' Test ssm_id generation '''

        ssm_id = _udf_uuid5_field('ssm', 'GRCh38','chr4','112382545','112382545','SNP','A','T')
        self.assertEqual(ssm_id, '3439eab1-0c63-50cd-bad7-1ae8ffa8aa01')

    def test_flat_fields(self):
        ''' Test mapping field flattener '''
        fields = flat_fields('../mappings/observation.yml')

        for field in ['src_vcf_id', 'center', 'tumor_sample_uuid']:
            self.assertIn(field, fields)

class TestBuilderSparkUtils(SparkTestCase):

    def test_struct_select(self):
        ''' Test mapping to select '''
        stmt = struct_select('observation.yml')

        df = MAFBuilder(TestConfig(), self.sqlContext).build()

        df_json = json.loads(df.select(*stmt).limit(1).toJSON().collect()[0])

        self.assertIn('center', df_json)
        self.assertIn('input_bam_file', df_json)
        self.assertIn('normal_bam_uuid', df_json['input_bam_file'])
