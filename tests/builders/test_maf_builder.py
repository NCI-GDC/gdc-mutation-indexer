import os
import re
import json
import yaml
from collections import Counter

from exports.builders import MAFBuilder
from tests_config import TestConfig

conf = TestConfig()

import pytest


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'test_index_class')
class TestMAFBuilder:

    def test_patch_url(self, sqlContext):
        ''' Test that s3 urls are patched correctly '''
        builder = MAFBuilder(conf, sqlContext)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        assert builder.patch_url(url1).startswith('s3a://')

    def test_combine(self, sqlContext):
        '''
        Test that mafs are combined correctly
        '''
        builder = MAFBuilder(conf, sqlContext)

        df = builder.combine(conf.maf_urls)
        assert df.count() == 18
        c = Counter([json.loads(item)['variant_caller'] for item
                     in df.select('variant_caller').toJSON().collect()])
        assert c['mutect2'] == 11
        assert c['muse'] == 7

    def test_schema(self, sqlContext):
        '''
        Test that maf has columns correctly renamed
        '''
        builder = MAFBuilder(conf, sqlContext)

        df = builder.combine(conf.maf_urls)
        df = builder.standardize_schema(df)

        path = os.path.join(conf.schemas_dir, 'maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']

        for field in maf_schema.keys():
            assert field in df.columns

    def test_ssm_id(self, sqlContext):
        '''
        Test that ssm_id column is created
        '''
        builder = MAFBuilder(conf, sqlContext)

        df = builder.combine(conf.maf_urls)
        df = builder.standardize_schema(df)
        df = builder.add_ssm_id(df)
        assert 'ssm_id' in df.columns

    def test_genomic_dna_change(self, maf_df):
        '''
        Test that the genomic_dna_change is created correctly
        '''

        assert 'genomic_dna_change' in maf_df.columns

        # Number of unique labels should be equal to the number of unique ssm
        assert (maf_df.select('ssm_id').distinct().count() ==
                maf_df.select('genomic_dna_change').distinct().count())

        labels = [r['genomic_dna_change'] for r
                  in maf_df.select('genomic_dna_change').collect()]

        assert 'chr1:g.32180498T>C' in labels
        assert 'chr2:g.182729892G>T' in labels
        assert 'chr9:g.2056812T>A' in labels

    def test_mutation_type(self, maf_df):
        '''
        Test that mutation_type is created properly
        '''

        assert 'mutation_type' in maf_df.columns

        # Should only have 1 type, 'Simple Somatic Mutation'
        assert maf_df.select('mutation_type').distinct().count() == 1
        assert (maf_df.select('mutation_type').collect()[0]['mutation_type'] ==
                'Simple Somatic Mutation')

    def test_variant_caller(self, maf_df):
        '''
        Test that variant caller is created properly
        '''
        assert 'variant_caller' in maf_df.columns
        muse = maf_df.where(maf_df.variant_caller == 'muse')
        assert muse.count() == 7
        mutect = maf_df.where(maf_df.variant_caller == 'mutect2')
        assert mutect.count() == 11

    def test_variant_process(self, maf_df):
        '''
        Test that variant process is created properly
        '''

        assert 'variant_process' in maf_df.columns
        assert maf_df.first()['variant_process'] == 'masked'

    def test_mutation_subtype(self, maf_df):
        '''
        Test that mutation_subtype is created properly
        '''

        assert 'mutation_subtype' in maf_df.columns

        # Should have as many distinct variants as subtypes
        assert (maf_df.select('variant_type').distinct().count() ==
                maf_df.select('mutation_subtype').distinct().count())

    def test_case_barcode(self, sqlContext):
        '''
        Test that ssm_id column is created
        '''
        builder = MAFBuilder(conf, sqlContext)

        df = builder.combine(conf.maf_urls)
        df = builder.standardize_schema(df)
        df = builder.extract_barcode(df)

        assert '_case_submitter_id' in df.columns
        assert (df.where(df.tumor_sample_barcode=='TCGA-A4-A6HP-01A-11D-A31X-10')
                  .select('_case_submitter_id')
                  .limit(1).collect()[0]._case_submitter_id == 'TCGA-A4-A6HP')

    def test_maf_field_types(self, maf_df):
        """
        Test that maf_df field types correspond to maf.yml
        """

        types = {'int': 'integer', 'bool': 'boolean', 'float': 'float'}
        for col in maf_df.schema:
            col_info = json.loads(col.json())
            if col_info['name'] in maf_df.schema:
                if 'type' in maf_df.schema[col_info['name']]:
                    assert col_info['type'] == types[maf_df.schema[col_info['name']]['type']]

    def test_maf_field_pattern(self, maf_df):
        """
        Test that maf_df field pattern correspond to maf.yml
        """

        for col in maf_df.schema:
            col_info = json.loads(col.json())
            colname = col_info['name']
            if colname in maf_df.schema:
                if 'pattern' in maf_df.schema[colname]:
                    pattern = maf_df.schema[colname]['pattern']
                    values = maf_df.select(colname)
                    for row in values.collect():
                        val = row[colname]
                        is_matching = re.search(pattern.replace('{}','.*'), val)
                        assert is_matching

