import unittest

from exports.mappings import Mapper, GeneMapper


class TestMapper(unittest.TestCase):
    
    def test_settings(self):
        mapper = Mapper()

        self.assertIn('dynamic', mapper.mapping)

    def test_properties(self):
        mapper = Mapper()

        props = mapper.load_properties('gene.yml')
        self.assertIn('properties', props)

        with self.assertRaises(AssertionError):
            mapper.load_properties('common_settings.yml')

        props = mapper.load_properties('gene.yml', nested=True)
        self.assertIn('type', props)
        self.assertEqual(props['type'], 'nested')

    def test_clean(self):
        mapper = Mapper()

        d = {
            'index': 'not_analyzed',
                '_meta':{ 'name': None},
                '_id': [],
                '_all': {},
                '_source': '',
                'descriptions': [''],
                'properties': {
                    'name': {
                        'type': 'text',
                        'index': 'not_analyzed'
                    },
                    'code': {
                        'type': 'long',
                        'index': 'analyzed',
                        'store': 'yes',
                        'index_analyzer': 'false'
                    }
                }
            }
        cleaned = mapper.clean(d)
        self.assertFalse('_meta' in cleaned)
        self.assertFalse('descriptions' in cleaned)
        self.assertFalse('_source' in cleaned)
        self.assertFalse('_all' in cleaned)
        self.assertFalse('_id' in cleaned)
        self.assertEqual(cleaned['properties']['name']['type'], 'keyword')
        self.assertEqual(cleaned['properties']['name']['index'], 'true')
        self.assertEqual(cleaned['properties']['name']['type'], 'keyword')
        self.assertEqual(cleaned['properties']['code']['store'], 'true')
        self.assertEqual(cleaned['properties']['code']['index'], 'true')
        self.assertFalse('index_analyzer' in cleaned['properties']['code'])


class TestGeneMapper(unittest.TestCase):

    def test_gene_map(self):
        mapper = GeneMapper()

        self.assertIn('properties', mapper.mapping)
        props = mapper.mapping['properties']

        self.assertIn('gene_chromosome', props)
        self.assertIn('gene_end', props)
        self.assertEqual('keyword', props['gene_id']['type'])
