from utils import SparkTestCase, JSONValidator
from exports.builders.utils import  percentile
import pytest
from random import randint
from tests_config import TestConfig

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

    def test_percentile(self):
        '''
        test the percentile util function
        '''
        l = randint(0, 100)
        if l % 2:
            l += 1
        v = [randint(0, 100) for x in range(l + 1)]
        sorted_v = sorted(v)
        self.assertEqual(percentile(v, 0), sorted_v[0])
        self.assertEqual(percentile(v, 50), sorted_v[l/2])
        self.assertEqual(percentile(v, 100), sorted_v[-1])

def test_json_validator_list_counts():
    test_json = {'a':
                    {'b': [1,
                            {2: 3, 'c': [4, 5, 6]},
                            3,
                            5]
                        },
                    'd': [1, 2, 3, [1, 2, 3, 4, 5]]
                    }
    true_output = {'root.a.b': len(test_json['a']['b']),
                    'root.a.b[1].c': len(test_json['a']['b'][1]['c']),
                    'root.d': len(test_json['d']),
                    'root.d[3]': len(test_json['d'][3])}

    output = JSONValidator.get_stats(test_json, mode='list')
    assert output == true_output

def test_json_validator_dict_keys():
    test_json = {'a':
                    {'b': [1,
                            {2: 3, 'c': [4, 5, 6]},
                            3,
                            5],
                        'k': 1
                        },
                    'd': [1, 2, 3, [1, 2, 3, 4, 5]]
                    }

    true_output = {'root': set(test_json.keys()),
                    'root.a': set(test_json['a'].keys()),
                    'root.a.b[1]': set(test_json['a']['b'][1].keys())}

    output = JSONValidator.get_stats(test_json, mode='dict')
    assert output == true_output

@pytest.mark.parametrize('path,value,outcome',
                         (('root.a.b', 4, True),
                         ('root.b[1].c', 1, False),
                         ('root.b[1]', 1, False),
                         ('root.d[3]', 5, True),
                         ('root.a.b[1].c', 3, True),
                         ))
def test_json_validator_validate_path(path, value, outcome):
    test_json = {'a':
                    {'b': [1,
                        {2: 3, 'c': [4, 5, 6]},
                        3,
                        5]
                    },
                'd': [1, 2, 3, [1, 2, 3, 4, 5]]
                }

    assert JSONValidator.validate_path(test_json, path, value) == outcome


@pytest.mark.parametrize('test_json,mismatches,mode',
                         (
                          ({'a': {'b': [1, {2: 3, 'c': [4, 5, 6]}, 3, 5] },
                            'd': [1, 2, 3, [1, 2, 3, 4, 5]]},
                           {}, 'list'),
                          ({'a': {'b': [1, {2: 3, 'c': [4, 5, 6]}, 3, 5] },
                            'd': [1, 2, 3, [1, 2, 3, 4, 5]]},
                           {}, 'dict'),
                          ({'a': {'b': [1, 3, 5] },
                            'd': [1, 3, [1, 2, 4, 5]]},
                           {'root.d': [3, 4], 'root.a.b[1].c': [None, 3],
                            'root.d[3]': [None, 5], 'root.a.b': [3, 4]},
                           'list'),
                             ({'a': {'d': [1, {2: 3, 'v': [4, 6]}, 3, 5], 'k': 1},
                            'c': [1, 5]},
                           {'root': [set(['d']), set(['c'])],
                            'root.a': [set(['b']), set(['k', 'd'])],
                            'root.a.b[1]': [None, set([2, 'c'])]},
                           'dict'),
                          )
                         )
def test_json_validator_find_mismatches(test_json, mismatches, mode):
    true_json = {'a': {'b': [1, {2: 3, 'c': [4, 5, 6]}, 3, 5] },
                 'd': [1, 2, 3, [1, 2, 3, 4, 5]]}
    assert JSONValidator.find_mismatches(test_json, true_json, mode) == mismatches
