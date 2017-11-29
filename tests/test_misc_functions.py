import pytest
from random import randint

from pyspark.sql.functions import lit
from exports.builders.utils import percentile, struct_select, extract_aas_position
from tests_config import TestConfig
from utils.true_stats import TestDataStats
from exports.builders.utils import (
    ssm_label,
    _udf_uuid5_field,
    sanitize_aa_change,
    sanitize_gene_aa_change,
    convert_empty_str_to_null_in_col,
    extract_impact,
    extract_score,
)

conf = TestConfig()


def create_df(sqlContext, values, column_name='values'):
    """
    Creates 1d mock dataframe from values list and column name
    """
    values = map(lambda v: (v,), values)
    return sqlContext.createDataFrame(values, [column_name])


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'es_client')
class TestMiscFunctions:

    def test_es_adapter(self, sqlContext):
        """
        Test that the elasticsearch-hadoop wrapper jar is loaded
        """
        # Fails if org.elasticsearch.hadoop.mr.LinkedMapWritable isnt in the path
        return (sqlContext.read.format("es")
                          .option('es.nodes', conf.source_es_host)
                          .option('es.nodes.resolve.hostname','false')
                          .option('es.resource.read', conf.graph_index)
                          .load(conf.graph_index))

    def test_percentile(self):
        """
        Test the percentile util function
        """
        l = randint(0, 100)
        if l % 2:
            l += 1
        v = [randint(0, 100) for i in range(l + 1)]
        sorted_v = sorted(v)
        assert percentile(v, 0) == sorted_v[0]
        assert percentile(v, 50) == sorted_v[l/2]
        assert percentile(v, 100) == sorted_v[-1]

    def test_struct_select(self):
        """
        Test mapping to select
        """

        indices = ['case_centric', 'gene_centric', 'ssm_centric',
                   'ssm_occurrence_centric']
        mappings = ['annotation', 'case', 'gene', 'observation', 'ssm',
                    'transcript']

        for mapping in mappings:
            for index in indices:
                print index, mapping
                stmt = struct_select(index, mapping)
                assert stmt

    def test_graph_index(self, es_client):
        """
        Test the test graph index fixture
        """
        assert es_client is not None
        assert conf.graph_index is not None
        assert es_client.count()['count'] > 0
        assert (es_client.get(index=conf.graph_index, doc_type='case',
                       id='d2748e35-4719-43c1-a533-b6b0cd9688c3')['_id']
                == 'd2748e35-4719-43c1-a533-b6b0cd9688c3')

    def test_properties(self):
        """
        Test that configuration properties are present
        """
        assert 's3_host' in dir(conf)
        assert 'es_host' in dir(conf)

    def test_index_prefix(self, es_client):
        """
        Test that index prefixes are determined correctly
        """

        index_name = 'case_centric'

        if es_client.indices.exists('gdc_r998_{}'.format(index_name)):
            es_client.indices.delete('gdc_r998_{}'.format(index_name))

        # Create a new index
        es_client.indices.create(index='gdc_r998_{}'.format(index_name))

        assert ('gdc_r999_{}'.format(index_name)
                == TestConfig().indices['case_centric'])
        es_client.indices.delete(index='gdc_r998_{}'.format(index_name))

    def test_sanitize_aa_change(self, sqlContext):
        # Fake input and expected output
        fake_input = ['a', 'p.b', 'cp.']
        expected_output = ['a', 'b', 'c']

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, 'aa_change')
        expected_df = create_df(sqlContext, expected_output, 'aa_change')

        # Test:
        assert sanitize_aa_change(df).collect() == expected_df.collect()

    def test_extract_impact_or_score(self, sqlContext):
        # Fake input and expected output
        fake_input = ['probably_damaging(0.1)', 'tolerated_low_confidence(2.1)', '']
        expected_impact_output = ['probably_damaging', 'tolerated_low_confidence', '']
        expected_score_output = [0.1, 2.1, None]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, 'field')
        expected_impact_df = create_df(sqlContext, expected_impact_output, 'field_impact')
        expected_score_df = create_df(sqlContext, expected_score_output, 'field_score')

        # Test:
        df = extract_impact(df, 'field', 'field_impact')
        df = extract_score(df, 'field', 'field_score')

        assert df.select('field_impact').collect() == expected_impact_df.collect()
        assert df.select('field_score').collect() == expected_score_df.collect()

    def test_sanitize_gene_aa_change(self, sqlContext):
        # Fake input and expected output
        fake_input = [['c', 'a', 'a', '', None, 'b', 'c', 'c']]
        expected_output = [['a', 'b', 'c']]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, 'gene_aa_change')
        expected_df = create_df(sqlContext, expected_output, 'gene_aa_change')

        # Test:
        assert sanitize_gene_aa_change(df).collect() == expected_df.collect()

    def test_convert_empty_str_to_null_in_col(self, sqlContext):
        # Fake input and expected output
        fake_input = ['a', '', 'c', '']
        expected_output = ['a', None, 'c', None]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, 'test')
        expected_df = create_df(sqlContext, expected_output, 'test')

        # Test:
        assert convert_empty_str_to_null_in_col(df, 'test').collect() == expected_df.collect()

    def test_aa_start_end(self, maf_df):
        """
        Test aa_start and aa_end extraction
        """
        new_df = maf_df.withColumn('aa_change', lit('p.L1201R'))
        new_df = extract_aas_position(new_df)

        assert 'aa_start' in new_df.columns
        assert 'aa_end' in new_df.columns
        aa_change = new_df.filter(new_df.aa_change == 'p.L1201R').select('aa_start', 'aa_end').collect()[0]
        assert aa_change['aa_start'] == 1201
        assert aa_change['aa_end'] == 1201

    def test_ssm_label(self):
        """
        Test ssm label generation
        """
        label = ssm_label('chr3', 'SNP', 41589825, '', 'A', 'T')
        assert label == 'chr3:g.41589825A>T'

        label = ssm_label('chr3', 'DEL', 41589825, '', 'A', '')
        assert label == 'chr3:g.41589825delA'

        label = ssm_label('chr3', 'INS', 41589825, 41589825, '', 'T')
        assert label == 'chr3:g.41589825_41589825insT'

        label = ssm_label('chr4', 'SNP', 112382545, '', 'A', 'T')
        assert label == 'chr4:g.112382545A>T'

    def test_uuid5(self):
        """
        Test uuid5 generation
        """

        ssm_id = _udf_uuid5_field('ssm', 'GRCh38', 'chr4',
                                  '112382545', '112382545',
                                  'SNP', 'A', 'T')
        assert ssm_id == '3439eab1-0c63-50cd-bad7-1ae8ffa8aa01'

        ssm_occ_id = _udf_uuid5_field('ssm_occurrence',
                                      '642a6e7d-8b15-5f93-9e29-22c9649e9058',
                                      '13afbde8-e5b5-4f3c-8a9d-daef71560005')
        assert ssm_occ_id == 'f4222c55-fea2-5b23-a204-482f33492800'

    @pytest.mark.parametrize('index', ['case_centric', 'gene_centric',
                                       'ssm_centric', 'ssm_occurrence_centric'])
    def test_test_data_stats(self, maf_df, index):
        """
        Test that TestDataStats loads test data and returns stats
        """
        data = TestDataStats.load_test_data(conf.input_dir)
        stats = TestDataStats.get_stats(maf_df, data, index)
