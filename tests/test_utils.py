from utils import SparkTestCase

import pytest
from conftest import ES_INDEX


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
        self.sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf={ "es.resource" : ES_INDEX })
