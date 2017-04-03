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
        Optionally, a `settings.yml` file may also be present which will be
        used to configure settings when creating the index

        :param dir_path: The path to the mapping's directory
        """
        self.index = index
        self._settings = None
        # Mappings keyed on the type
        self.type_mappings = {}

        for f in os.listdir(os.path.join('exports', 'gdc-models',
                                         'es-models', self.index)):
            if f.endswith('.mapping.yaml'):
                # Resolve type from file name
                doc_type = f.split('.')[0]
                # Get path and load the mapping
                path = self.get_resource_sring(f)
                self.type_mappings[doc_type] = yaml.safe_load(path)

    def get_resource_sring(self, path):
        resource_path = os.path.join('gdc-models', 'es-models', self.index,
                                     path)
        return pkg_resources.resource_string('exports', resource_path)

    def create_index_settings(self):
        """
        Will create a dict used to make an index including the mappings
        for each type and the settings, if there is a `settings.yml` file
        """
        d = {
            "mappings": self.type_mappings,
            "settings": None
        }
        # Add settings if the file exists
        settings_path = os.path.join(self.index, 'settings.yml')
        if os.path.exists(settings_path):
            settings = self.get_resource_string(settings_path)
            d['settings'] = settings
        return d
