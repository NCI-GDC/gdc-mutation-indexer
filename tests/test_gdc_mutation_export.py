import pytest
from elasticsearch import Elasticsearch
from utils import SparkTestCase
from config import TestConfig
from exports import GDCMutationExport


@pytest.mark.usefixtures('test_index_class')
class TestGDCMutationExport(SparkTestCase):

    def setUp(self):
        super(TestGDCMutationExport, self).setUp()
        self.exporter = GDCMutationExport(self.sc, self.sqlContext, TestConfig)

    def test_index_prefix(self):
        '''
        Test that index prefixes are determined correctly
        '''

        index_name = 'test_prefix__'

        if self.es.indices.exists('gdc_r998_{}'.format(index_name)):
            self.es.indices.delete('gdc_r998_{}'.format(index_name))

        # Create a new index
        self.es.indices.create(index='gdc_r998_{}'.format(index_name))
        self.assertEqual('gdc_r999_{}'.format(index_name),
                        self.exporter.get_index_prefix(index_name))
        self.es.indices.delete(index='gdc_r998_{}'.format(index_name))

        self.assertEqual('gdc_r0_{}'.format(index_name),
                        self.exporter.get_index_prefix(index_name))
