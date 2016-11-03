import yaml


class Mapper(object):
    '''
    A mapper is responsible for generating the mapping for a doc type
    '''

    def __init__(self):
        self.mapping = self.build_mapping()

    def build_mapping(self):
        '''
        Constructs an elastic search mapping
        '''
        mapping = {}
        mapping.update(self.settings)
        return mapping

    def load_properties(self, path, nested=False):
        '''
        Loads an ES properties mapping from a yaml file
        '''
        with open(path) as f:
            properties = yaml.load(f)
        assert 'properties' in properties, 'File must contain properties'
        if nested:
            properties['type'] = 'nested'
        return properties

    @property
    def settings(self):
        with open('exports/mappings/common_settings.yaml') as f:
            settings = yaml.load(f)
        return settings
