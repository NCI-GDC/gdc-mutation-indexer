from utils import SparkTestCase
import pytest
from config import TestConfig

conf = TestConfig

@pytest.mark.usefixtures('test_index')
class TestUtils(SparkTestCase):

    def test_setup(self):
        '''
        Test that spark was setup and context exists
        '''
        self.assertEqual(self.sc.appName, 'TestUtils')

    def test_es_adapter(self):
        '''
        Test that the elasticsearch-hadoop wrapper jar is loaded
        '''
        # Fails if org.elasticsearch.hadoop.mr.LinkedMapWritable isnt in the path
        return self.sqlContext.read.format("es")\
            .option('es.nodes', conf.source_es_host)\
            .option('es.nodes.resolve.hostname','false')\
            .option('es.resource.read', conf.graph_index)\
            .load(conf.graph_index)
