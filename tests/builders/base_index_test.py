import json
import os
from elasticsearch import Elasticsearch

from tests_config import TestConfig
conf = TestConfig()


class BaseIndexTest:
    """
    Abstract wrapper around ['gene_centric', 'case_centric',
                             'ssm_centric', 'ssm_occurrence_centric'] tests
    """

    def __init__(self, builder, test_config):
        self.builder = builder
        self.conf = test_config
        self.index = builder.index_name
        self.id_field = '{}_id'.format(self.index.replace('_centric', ''))
        self.output_dir = os.path.join(self.conf.output_dir, self.index)
        self.debug = self.conf.print_data_errors

    def generate_index(self, sqlContext, maf_df):
        """
        Generates index corresponding to self.builder
        Returns Elasticsearch instance
        """
        es = Elasticsearch(self.conf.es_host, port=self.conf.es_port)

        self.builder(self.conf, sqlContext).build(maf_df).load()

        return es

    def get_docs_to_compare(self, es_index, filename):
        """
        Returns true document loaded from :filename
        and a corresponding built document from elasticsearch
        """
        # Compare each true output document with document in ES:
        with open(os.path.join(self.output_dir, filename), 'r') as f:
            true_doc = json.loads(f.read())

        es_doc = es_index.get(index=self.conf.indices[self.index],
                              id=filename)['_source']

        assert self.id_field in es_doc.keys()

        return es_doc, true_doc

    def say(self, string):
        """
        If not self.debug does nothing
        Else: prints decorated string
        """
        if string[0] == '\n':
            print ''
            self.say(string[1:])
        else:
            if self.debug:
                print '~{}~{}'.format(self.index, string)
