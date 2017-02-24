import os
import uuid
import json
import pytest
import unittest

from utils import SparkTestCase
from exports.builders import (
    MAFBuilder,
    GeneCentricBuilder,
    CaseCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)
from exports.builders.utils import (
    ssm_uuid,
    ssm_label,
    flat_fields,
    struct_select,
    _udf_uuid5_field
)
from config import TestConfig


conf = TestConfig()



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

    def test_uuid5(self):
        ''' Test uuid5 generation '''

        ssm_id = _udf_uuid5_field('ssm', 'GRCh38','chr4','112382545','112382545','SNP','A','T')
        self.assertEqual(ssm_id, '3439eab1-0c63-50cd-bad7-1ae8ffa8aa01')

        ssm_occ_id = _udf_uuid5_field('ssm_occurrence',
                                      '642a6e7d-8b15-5f93-9e29-22c9649e9058',
                                      '13afbde8-e5b5-4f3c-8a9d-daef71560005')
        self.assertEqual(ssm_occ_id, 'f4222c55-fea2-5b23-a204-482f33492800')

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
