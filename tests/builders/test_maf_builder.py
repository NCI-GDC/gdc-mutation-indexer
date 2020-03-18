import os
import re
import json
from collections import Counter
from contextlib import contextmanager

import yaml
import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import ArrayType, StringType

from exports.builders import MAFBuilder
from tests_config import TestConfig

conf = TestConfig()


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize('url, expected, behavior', [
    ('s3a://varscan-10/bar.somaticsniper.baz', 'somaticsniper', does_not_raise()),
    ('s3a://varscan-10/bar.mutect.baz', 'mutect2', does_not_raise()),
    ('s3a://varscan-10/bar.muse.baz', 'muse', does_not_raise()),
    ('s3a://varscan-10/bar.varscan.baz', 'varscan', does_not_raise()),
    ('s3a://varscan-10/bar.FM-AD_SNV.baz', 'FM Simple Somatic Mutation', does_not_raise()),
    ('s3://foo-bar/bar.something.baz', None, pytest.raises(Exception)),
    ('s3a://varscan-10/bar.FM-AD.baz', None, pytest.raises(Exception)),
    ('s3a://varscan-10/bar.muse.varscan.baz', None, pytest.raises(Exception)),
])
def test_maf_builder_get_caller(sqlContext, maf_df, url, expected, behavior):
    builder = MAFBuilder(conf, sqlContext)

    with behavior:
        result = builder.get_caller(url)

        assert result == expected


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestMAFBuilder:

    @pytest.fixture
    def expected_counts(self, sqlContext):
        builder = MAFBuilder(conf, sqlContext)
        expected_counts = {}
        for maf_file in conf.maf_urls:
            pipeline = maf_file.split('.')[2]
            if pipeline == 'mutect':
                pipeline = 'mutect2'

            df = builder.combine([maf_file])
            expected_counts.setdefault(pipeline, 0)
            expected_counts[pipeline] += df.count()
        yield expected_counts

    @pytest.fixture
    def maf_schema(self):
        path = os.path.join(conf.schemas_dir, 'maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']

        return maf_schema

    @pytest.fixture
    def annotation_schemas(self, sqlContext):
        builder = MAFBuilder(conf, sqlContext)
        return builder.get_annotation_schemas()

    def test_patch_url(self, sqlContext):
        ''' Test that s3 urls are patched correctly '''
        builder = MAFBuilder(conf, sqlContext)
        url1 = 's3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt'
        assert builder.patch_url(url1).startswith('s3a://')

    def test_combine(self, sqlContext, expected_counts):
        '''
        Test that mafs are combined correctly
        '''
        builder = MAFBuilder(conf, sqlContext)

        combined_df = builder.combine(conf.maf_urls)

        # Test total number of lines
        assert combined_df.count() == sum(expected_counts.values())

        c = Counter([json.loads(item)['variant_caller']
                     for item in (combined_df.select('variant_caller')
                                             .toJSON().collect())])

        # Test number of lines for each pipeline (i.e. 'variant_caller')
        for pipeline, count in expected_counts.items():
            assert c[pipeline] == count

    def test_schema(self, sqlContext, maf_schema):
        '''
        Test that maf has columns correctly renamed
        '''
        builder = MAFBuilder(conf, sqlContext)

        # combine() should standardize the columns while loading the data
        df = builder.combine(conf.maf_urls)

        for field in maf_schema.keys():
            assert field in df.columns

    def test_standardize_schema_field_order(self, sqlContext, maf_schema):
        '''
        Confirm that standardize_schema standardizes the field order

        This verifies that we can safely union mafs together
        '''
        builder = MAFBuilder(conf, sqlContext)

        # Bypass combine() so the dataframe isn't already standardized.
        df = (
            builder.file_to_df(conf.maf_urls[0])
            .withColumn('caller', lit('variant_caller'))
            .withColumn('acl', lit(None))
        )
        columns = df.columns

        sort_df = builder.standardize_schema(df.select(*sorted(columns)))
        assert sort_df.columns == maf_schema.keys()

        reverse_df = builder.standardize_schema(df.select(*reversed(columns)))
        assert reverse_df.columns == maf_schema.keys()

        extra_columns = columns[:]
        extra_columns.insert(0, lit('asdf').alias('extra'))
        extra_columns.insert(4, lit(300).alias('extraneous'))
        extra_columns.append(lit(None).alias('superfluous'))
        extra_df = builder.standardize_schema(df.select(*extra_columns))
        assert extra_df.columns == maf_schema.keys()

    def test_standardize_schema_missing_field(self, sqlContext, maf_schema):
        '''
        Test standardize_schema's handling of missing fields

        Fields that default_to_none should be filled in with None columns;
        other missing fields should trigger an exception
        '''
        builder = MAFBuilder(conf, sqlContext)

        # The raw dataframe is missing a couple columns, so it should
        # initially fail standardization.
        df = builder.file_to_df(conf.maf_urls[0])

        try:
            builder.standardize_schema(df)
            assert False, 'Builder accepted df missing required columns'
        except KeyError:
            pass

        # If we tell the builder to supply None values for the missing columns,
        # then it should fill in those columns and standardize successfully.
        standardized_df = builder.standardize_schema(
            df, default_to_none=['caller', 'acl'])
        assert standardized_df.columns == maf_schema.keys()

    def test_ssm_id(self, maf_df):
        '''
        Test that ssm_id column is created
        '''
        assert 'ssm_id' in maf_df.columns

    def test_cosmic_id(self, maf_df):
        '''
        Test that cosmic_id column is created and is ArrayType(StringType())
        '''
        assert 'cosmic_id' in maf_df.columns
        data_type = maf_df.schema['cosmic_id'].dataType
        assert isinstance(data_type, ArrayType)
        assert isinstance(data_type.elementType, StringType)

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

    def test_variant_caller(self, maf_df, expected_counts):
        '''
        Test that variant caller is created properly
        '''
        assert 'variant_caller' in maf_df.columns
        for variant_caller, expected_count in expected_counts.items():
            count = maf_df.where(maf_df.variant_caller == variant_caller).count()
            assert count == expected_count

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
        df = builder.extract_barcode(df)

        assert '_case_submitter_id' in df.columns
        assert (df.where(df.tumor_sample_barcode
                         == 'TCGA-A4-A6HP-01A-11D-A31X-10')
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
                        is_matching = re.search(pattern.replace('{}', '.*'),
                                                val)
                        assert is_matching

    def test_annotations(self, annotation_schemas, maf_df):
        for schema in annotation_schemas:
            for k in schema.keys():
                assert k in maf_df.columns
