import os
import uuid
from elasticsearch import Elasticsearch


class BaseConfig(object):
    # The Spark application name
    app_name = 'GDC_Mutation_Export'

    api_host = 'http://api.service.consul'
    signpost_host = 'http://signpost.service.consul'
    s3_host = 'http://cleversafe.service.consul'
    # This is the cluster where document will be loaded into
    es_host = 'http://elasticsearchvis.service.consul'
    es_port = 9200

    # Index names, these also double as document type names
    # If name is None, the index will not be built
    index_names = {
        'case_centric':         'case_centric',
        'gene_centric':         'gene_centric',
        'ssm_centric':          'ssm_centric',
        'ssm_ocurrence_centric':'ssm_occurrence_centric'
    }

    # Index revision number, will be determined automatically if not specified
    revision = None

    # Used for loading case/graph documents from a different es cluster
    source_es_host = 'http://elasticsearch.service.consul'
    source_es_port = 9200
    graph_document = 'case'
    graph_index = 'gdc_from_graph'

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The name of the combined maf file
    maf_path = 's3a://test/combined_mafs.csv'
    # Whether to save the maf file or discard it when done
    maf_keep = True
    # Use combined maf if it already exists
    maf_use_existing = True
    # Whether to overwrite the combined maf file if it exists
    maf_overwrite = True

    # Case load settings
    case_fields = ','.join(['case_id',
                            'state',
                            'submitter_id',
                            '*_datetime',
                            '*_ids',
                            'project.*',
                            'program.*',
                            'demographic.*'])
    case_arrays = ','.join(['*_ids',
                            'diagnoses',
                            'summary.data_categories'])

    def __init__(self):
        self.indices = self.get_index_prefixes()

    def get_index_prefixes(self):
        '''
        Uses the version specified in the config, or will resolve the next
        version number by looking for an existing index and incrementing by one

        Eg:
            No indices exist in ES:
                index_name='case_centric' -> gdc_r0_case_centric

            gdc_r1_case_centric and gdc_r6_case_centric exist in ES:
                index_name='case_centric' -> gdc_r7_case_centric
        '''
        es = Elasticsearch(self.es_host, port=self.es_port)

        def get_prefix(index_name):
            indices = es.indices.get_alias().keys()
            versions = [ int(v.split('_')[1].replace('r',''))
                            for v in indices if v.endswith(index_name) and v[:4]=='gdc_' ]
            # If there is no index with this name in it
            if versions == []:
                version = 0
            else:
                version = max(versions) + 1

            prefix = 'gdc_r{}_{}'.format(version, index_name)
            return prefix

        indices = { k: get_prefix(v)
                            for k,v in self.index_names.items()
                            if v is not None }
        return indices


class TestConfig(BaseConfig):
    es_host = 'localhost'
    source_es_host = 'localhost'
    graph_index = 'test_graph_index__'

    index_names = {
        'case_centric':         'test_case_centric__',
        'gene_centric':         'test_gene_centric__',
        'ssm_centric':          'test_ssm_centric__',
        'ssm_ocurrence_centric':'test_ssm_occurrence_centric__'
    }

    maf_path = 'file:///test_mafs.csv'
    keep_indices = True
    maf_keep = False
    maf_use_existing = False

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests')
    data_dir = os.path.join(test_dir, 'data')


configs = {
    'BaseConfig': BaseConfig,
    'TestConfig': TestConfig
}


