import os
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
        path = os.path.join(os.path.dirname(__file__),'../mappings',path)
        with open(path) as f:
            properties = yaml.load(f)
        assert 'properties' in properties, 'File must contain properties'
        if nested:
            properties['type'] = 'nested'
        return properties

    @property
    def settings(self):
        path = os.path.join(os.path.dirname(__file__),'../mappings','common_settings.yml')
        with open(path) as f:
            settings = yaml.load(f)
        return settings

    def clean(self, d):
        '''
        Patches old elasticsearh mappings to 5.0
        string type -> text type
        store: yes -> store: true
        removes index_analyzer fields
        index: not_analyzed -> index: true, type: keyword
        index: analyzed -> index: true
        remove descriptions, _meta, _source, _id, _all fields
        '''
        if not isinstance(d, (dict, list)):
            return d
        if isinstance(d, list):
            return [v for v in (self.clean(v) for v in d) if v]

        if 'type' in d and d['type'] == 'string':
            d['type'] = 'text'
        if 'store' in d and d['store'] == 'yes':
            d['store'] = 'true'
        if 'index_analyzer' in d:
            del d['index_analyzer']
        if 'index' in d and d['index'] == 'not_analyzed':
            d['index'] = 'true'
            d['type'] = 'keyword'
        elif 'index' in d and d['index'] == 'analyzed':
            d['index'] = 'true'

        for f in ['descriptions', '_meta', '_source', '_id', '_all']:
            if f in d:
                del d[f]
        return {k: v for k, v in ((k, self.clean(v)) for k, v in d.items()) if v is not 'id_search'}
