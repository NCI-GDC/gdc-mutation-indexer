import unittest
import pytest

from elasticsearch import Elasticsearch


@pytest.mark.usefixtures('test_index_class')
class TestFixtures(unittest.TestCase):

    def test_graph_index(self):
        '''
        Test the test graph index fixture
        '''
        self.assertNotEqual(self.es, None)
        self.assertNotEqual(self.config.graph_index, None)
        self.assertGreater(self.es.count()['count'], 0)
        self.assertEqual(
            self.es.get(index=self.config.graph_index, doc_type='case',
                        id='d2748e35-4719-43c1-a533-b6b0cd9688c3')['_id'],
                        'd2748e35-4719-43c1-a533-b6b0cd9688c3')
