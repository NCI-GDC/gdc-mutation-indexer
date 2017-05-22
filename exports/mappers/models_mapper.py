import os
import yaml
import pkg_resources


class ModelMapper(object):
    """
    The model mapper will create mapping and index settings for a document type

    The expected layout of the model directory should like like::

        my_index/
            my_type.mapping.yml
            settings.yml (optional)
    """

    def __init__(self, index):
        """
        Will create a mapper for a given index.
        The :param:index should have a corresponding directory in
        `gdc-models/es-models/` wherein each type should have a
        `my_type.mapping.yml` specifying its mapping.
        """
        self.index = index

        # Mappings keyed on the type
        self.type_mappings = {}

        filename = '{}.mapping.yaml'.format(index)

        # Load the mapping
        resource = self.get_resource_string(filename)
        self.type_mappings[index] = yaml.safe_load(resource)

    def get_resource_string(self, path):
        resource_path = os.path.join('gdc-models', 'es-models', self.index,
                                     path)
        return pkg_resources.resource_string('exports', resource_path)

    def create_index_settings(self):
        """
        Will create a dict used to make an index including the mappings
        for each type and the settings, if there is a `settings.yml` file
        """

        # Load common settings file:
        path = os.path.join('schemas', 'common_settings.yml')
        common_settings_file = pkg_resources.resource_string('exports', path)
        common_settings = yaml.safe_load(common_settings_file)

        # Unpack common_settings file:
        mappings = common_settings.pop('mappings', {})
        doctype_settings = common_settings.pop('common_mapping_settings', {})
        settings = common_settings.pop('settings', {})
        assert common_settings == {}

        # Add mappings from common_settings.yml:
        self.type_mappings.update(mappings)

        # Add default common doctype settings from common_settings.yml
        # (only set if it's not present, don't overwrite if already set)
        for doctype in self.type_mappings:
            for k, v in doctype_settings.items():
                self.type_mappings[doctype].setdefault(k, v)

        # Populate
        final_mapping = {
            "mappings": self.type_mappings,
            "settings": settings
        }

        # Add settings from '{index_name}.settings.yaml' file
        settings_string = self.get_resource_string('settings.yaml')
        if settings_string is not None:
            custom_settings = yaml.safe_load(settings_string)
            final_mapping['settings'].update(custom_settings)

        return final_mapping

    @property
    def paths_map(self):
        return {
            'observation': {
                'case_centric':
                    ['case_centric', 'properties', 'gene', 'properties', 'ssm',
                     'properties', 'observation'],
                'gene_centric':
                    ['gene_centric', 'properties', 'case', 'properties', 'ssm',
                     'properties', 'observation'],
                'ssm_centric':
                    ['ssm_centric', 'properties', 'occurrence', 'properties',
                     'case', 'properties', 'observation'],
                'ssm_occurrence_centric':
                    ['ssm_occurrence_centric', 'properties', 'case', 'properties',
                     'observation'],
            },
            'annotation': {
                'case_centric':
                    ['case_centric', 'properties', 'gene', 'properties', 'ssm',
                     'properties', 'consequence', 'properties', 'transcript',
                     'properties', 'annotation'],
                'gene_centric':
                    ['gene_centric', 'properties', 'case', 'properties', 'ssm',
                     'properties', 'consequence', 'properties', 'transcript',
                     'properties', 'annotation'],
                'ssm_centric':
                    ['ssm_centric', 'properties', 'consequence', 'properties',
                     'transcript', 'properties', 'annotation'],
                'ssm_occurrence_centric':
                    ['ssm_occurrence_centric', 'properties', 'ssm', 'properties',
                     'consequence', 'properties', 'transcript', 'properties',
                     'annotation'],
            },
            'transcript': {
                'case_centric': ['case_centric', 'properties', 'gene',
                                 'properties', 'ssm', 'properties',
                                 'consequence', 'properties', 'transcript'],
                'gene_centric': ['gene_centric', 'properties', 'case',
                                 'properties', 'ssm', 'properties',
                                 'consequence', 'properties', 'transcript'],
                'ssm_centric': ['ssm_centric', 'properties', 'consequence',
                                'properties', 'transcript'],
                'ssm_occurrence_centric': ['ssm_occurrence_centric', 'properties',
                                           'ssm', 'properties', 'consequence',
                                           'properties', 'transcript'],
            },
            'case': {
                'case_centric': ['case_centric'],  # ??
                'gene_centric': ['gene_centric', 'properties', 'case'],
                'ssm_centric': ['ssm_centric', 'properties', 'occurrence',
                                'properties', 'case'],
                'ssm_occurrence_centric': ['ssm_occurrence_centric',
                                           'properties', 'case'],
            },
            'gene': {
                'case_centric': ['case_centric', 'properties', 'gene'],
                'gene_centric': ['gene_centric'],  # ??
                'ssm_centric': ['ssm_centric', 'properties', 'consequence',
                                'properties', 'transcript', 'properties', 'gene'],
                'ssm_occurrence_centric': ['ssm_occurrence_centric', 'properties',
                                           'ssm', 'properties', 'consequence',
                                           'properties', 'transcript', 'properties',
                                           'gene'],
            },
            'ssm': {
                'case_centric': ['case_centric', 'properties', 'gene',
                                 'properties', 'ssm'],
                'gene_centric': ['gene_centric', 'properties', 'case',
                                 'properties', 'ssm'],
                'ssm_centric': ['ssm_centric'],  # ??
                'ssm_occurrence_centric': ['ssm_occurrence_centric', 'properties',
                                           'ssm'],
            },

        }

    @property
    def exclude_map(self):
        return {
            'case': {
                'case_centric': ['gene', 'transcripts'],
                'gene_centric': ['ssm'],
                'ssm_centric': ['observation'],
                'ssm_occurrence_centric': ['observation'],
            },
            'gene': {
                'gene_centric': ['case', 'transcripts'],
                'case_centric': ['ssm'],
                'ssm_centric': [],
                'ssm_occurrence_centric': [],
            },
            'ssm': {
                'case_centric': ['consequence', 'observation'],
                'gene_centric': ['consequence', 'observation'],
                'ssm_centric': ['consequence', 'occurrence'],
                'ssm_occurrence_centric': ['consequence'],
            },
            'transcript': {
                'case_centric': ['annotation'],
                'gene_centric': ['annotation'],
                'ssm_centric': ['annotation', 'gene'],
                'ssm_occurrence_centric': ['annotation', 'gene'],
            },
            'observation': {
                'case_centric': [],
                'gene_centric': [],
                'ssm_centric': [],
                'ssm_occurrence_centric': [],
            },
            'annotation': {
                'case_centric': [],
                'gene_centric': [],
                'ssm_centric': [],
                'ssm_occurrence_centric': [],
            }
        }
