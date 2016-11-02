import unittest
import pytest

from elasticsearch import Elasticsearch


@pytest.mark.usefixtures('test_index')
class TestFixtures(unittest.TestCase):

    def test_graph_index(self):
        '''
        Test the test graph index fixture
        '''
        self.assertNotEqual(self.es, None)
        self.assertNotEqual(self.graph_index, None)
        self.assertGreater(self.es.count()['count'], 0)
        self.assertEqual(
            self.es.get(index=self.graph_index, doc_type='case',
                        id='a53e9117-0a49-40d8-af40-7867370b671a')['_id'],
                        'a53e9117-0a49-40d8-af40-7867370b671a')
