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
        self.debug = self.conf.print_data_errors

    def generate_index(self, sqlContext, maf_df):
        """
        Generates index corresponding to self.builder
        Returns Elasticsearch instance
        """
        es = Elasticsearch(self.conf.es_host, port=self.conf.es_port)

        self.builder(self.conf, sqlContext).build(maf_df).load()

        return es

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
