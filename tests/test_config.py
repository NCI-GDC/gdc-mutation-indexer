import pytest
import unittest

from config import BaseConfig, TestConfig


@pytest.mark.usefixtures('test_index_class')
class TestConfigUtils(unittest.TestCase):

    def test_properties(self):
        ''' Test that configuration properties are present '''
        conf = TestConfig()
        self.assertIn('api_host', dir(conf))
        self.assertIn('signpost_host', dir(conf))
        self.assertIn('s3_host', dir(conf))
        self.assertIn('es_host', dir(conf))

    def test_index_prefix(self):
        '''
        Test that index prefixes are determined correctly
        '''

        index_name = 'test_case_centric__'

        if self.es.indices.exists('gdc_r998_{}'.format(index_name)):
            self.es.indices.delete('gdc_r998_{}'.format(index_name))

        # Create a new index
        self.es.indices.create(index='gdc_r998_{}'.format(index_name))
        conf = TestConfig()
        self.assertEqual('gdc_r999_{}'.format(index_name),
                             conf.indices['case_centric'])
        self.es.indices.delete(index='gdc_r998_{}'.format(index_name))
