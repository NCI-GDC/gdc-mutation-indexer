import unittest

from exports.mappings import Mapper, GeneMapper


class TestMapper(unittest.TestCase):
    
    def test_settings(self):
        mapper = Mapper()

        self.assertIn('_all', mapper.mapping)
        self.assertIn('_source', mapper.mapping)
        self.assertIn('dynamic', mapper.mapping)
        self.assertFalse(mapper.mapping['_all']['enabled'])

    def test_properties(self):
        mapper = Mapper()

        props = mapper.load_properties('gene.yaml')
        self.assertIn('properties', props)

        with self.assertRaises(AssertionError):
            mapper.load_properties('common_settings.yaml')

        props = mapper.load_properties('gene.yaml', nested=True)
        self.assertIn('type', props)
        self.assertEqual(props['type'], 'nested')


class TestGeneMapper(unittest.TestCase):

    def test_gene_map(self):
        mapper = GeneMapper()

        self.assertIn('_all', mapper.mapping)
        self.assertIn('_source', mapper.mapping)
        self.assertIn('dynamic', mapper.mapping)
        self.assertFalse(mapper.mapping['_all']['enabled'])

        self.assertIn('_id', mapper.mapping)
        self.assertEqual('gene_id', mapper.mapping['_id']['path'])

        self.assertIn('properties', mapper.mapping)
        props = mapper.mapping['properties']

        self.assertIn('gene_chromosome', props)
        self.assertIn('gene_end', props)
        self.assertEqual('string', props['gene_id']['type'])
        self.assertEqual('not_analyzed', props['symbol']['index'])
