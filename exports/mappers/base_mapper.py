import os
import yaml
import pkg_resources


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
        return mapping

    def load_properties(self, path, nested=False):
        '''
        Loads an ES properties mapping from a yaml file
        '''
        resource_package = 'exports'
        resource_path = '/'.join(('mappings', path))

        properties = yaml.safe_load(pkg_resources.resource_string(resource_package, resource_path))
 
        assert 'properties' in properties, 'File must contain properties'
        if nested:
            properties['type'] = 'nested'

        self.rm_maf_cols(properties)

        return properties

    def change_props_to_keyword(self, paths, data):
        for path in paths:
            prop = data['properties']

            for key in path.split('.'):
                prop = prop[key]

            if 'fields' in prop:
                del prop['fields']

            prop['type'] = 'keyword'

    @property
    def settings(self):
        resource_package = 'exports'
        resource_path = '/'.join(('mappings', 'common_settings.yml'))

        settings = yaml.safe_load(pkg_resources.resource_string(resource_package, resource_path))
        if 'mappings' in settings and type(settings['mappings']) is dict:
            settings['mappings'].update(self.build_mapping())
        else:
            settings['mappings'] = self.build_mapping()
        return settings

    def rm_maf_cols(self, d):
        '''
        '''
        if type(d) is dict:
            if 'default' in d:
                del d['default']
            for k,v in d.items():
                self.rm_maf_cols(v)
        if type(d) is list:
            for v in d:
                self.rm_maf_cols(v)

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
