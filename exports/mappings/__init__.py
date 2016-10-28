import yaml

class Mapper(object):

    def __init__(self):
        self.mapping = self.build_mapping()

    def build_mapping(self):
        '''
        Constructs an elastic search mapping
        '''
        mapping = {}
        mapping.update(self.settings)
        return mapping

    def load_properties(self, path):
        '''
        Loads an ES properties mapping from a yaml file
        '''
        with open(path) as f:
            properties = yaml.load(f)
        assert 'properties' in properties, 'File must contain properties'
        return properties

    @property
    def settings(self):
        with open('exports/mappings/common_settings.yaml') as f:
            settings = yaml.load(f)
        return settings


class GeneMapper(Mapper):

    def build_mapping(self):
        mapping = Mapper.build_mapping(self)
        mapping.update({"_id": { "path": "gene_id" }})
        mapping.update(self.load_properties('exports/mappings/gene.yaml'))
        return mapping
