import os
import uuid
from elasticsearch import Elasticsearch


class BaseConfig(object):
    # The Spark application name
    app_name = 'GDC_Mutation_Export'
    #spark_master = 'spark://dev-master-av2-dev2-dkolbman-notebook-0:7077'
    spark_master = 'local[1]'

    api_host = os.getenv('API_HOST', 'http://api.service.consul')
    signpost_host = os.getenv('SIGNPOST_HOST', 'http://signpost.service.consul')
    s3_host = os.getenv('S3_HOST', 'http://cleversafe.service.consul')
    # This is the cluster where document will be loaded into
    es_host = os.getenv('ES_HOST', 'http://elasticsearchvis.service.consul')
    es_port = os.getenv('ES_PORT', 9200)
    es_user = os.getenv('ES_USER', '')
    es_pass = os.getenv('ES_PASS', '')

    # Index names, these also double as document type names
    # If name is None, the index will not be built
    index_names = {
        'case_centric':         'case_centric',
        'gene_centric':         'gene_centric',
        'ssm_centric':          'ssm_centric',
        'ssm_occurrence_centric':'ssm_occurrence_centric'
    }

    # Index revision number, will be determined automatically if not specified
    revision = None

    # Used for loading case/graph documents from a different es cluster
    source_es_host = os.getenv('SOURCE_ES_HOST',
                               'http://elasticsearchvis.service.consul')
    source_es_port = os.getenv('SOURCE_ES_PORT', 9200)
    graph_index = os.getenv('SOURCE_ES_INDEX', 'gdc_from_graph_5')
    graph_document = os.getenv('SOURCE_ES_DOCUMENT', 'case')

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The location of the gene model json
    gene_model_path = 's3a://test/genes.json'


    # Locations of MAFs to combine. If none, all public paths listed on the
    # the portal will be combined and used
    maf_urls = ['s3a://test/258c6357-4348-4b95-a266-03f50d862d9f/TCGA.KICH.somaticsniper.c652b1a7-2c9a-4d38-b317-c401b396a73e.somatic.maf.gz']
    # The location of the combined maf file
    maf_path = 's3a://test/uat_mafs.csv'
    # Whether to save the maf file or discard it when done
    maf_keep = False
    # Use combined maf if it already exists
    maf_use_existing = False
    # Whether to overwrite the combined maf file if it exists
    maf_overwrite = True

    # Case load settings
    case_exclude_fields = ','.join(['samples',
                                    'annotations',
                                    'exposures',
                                    'family_histories',
                                    'files'])
    case_arrays = ','.join(['*_ids'])

    def __init__(self):
        self.indices = self.get_index_prefixes()

    def get_index_prefixes(self):
        """
        Uses the version specified in the config, or will resolve the next
        version number by looking for an existing index and incrementing by one

        Eg:
            No indices exist in ES:
                index_name='case_centric' -> gdc_r0_case_centric

            gdc_r1_case_centric and gdc_r6_case_centric exist in ES:
                index_name='case_centric' -> gdc_r7_case_centric
        """
        es = Elasticsearch(self.es_host,
                           port=self.es_port,
                           http_auth=(self.es_user, self.es_pass))

    def get_prefix(index_name):
        indices = es.indices.get_alias().keys()
        versions = [int(v.split('_')[1].replace('r',''))
        for v in indices
        if v.endswith(index_name) and v[:4] == 'gdc_']
            # If there is no index with this name in it
            if versions == []:
                version = 0
            else:
                version = max(versions) + 1

            prefix = 'gdc_r{}_{}'.format(version, index_name)
            return prefix

        indices = {k: get_prefix(v)
                   for k, v in self.index_names.items() if v is not None}

        return indices


class TestConfig(BaseConfig):
    root_dir = os.path.dirname(__file__)
    print root_dir
    test_dir = os.path.join(root_dir, 'tests')
    data_dir = os.path.join(test_dir, 'data')

    exp_data_dir = os.path.join(os.path.dirname(test_dir), 'exports', 'data')

    es_host = 'http://localhost'
    source_es_host = 'http://localhost'
    graph_index = 'test_graph_index__'

    index_names = {
        'case_centric':           'test_case_centric__',
        'gene_centric':           'test_gene_centric__',
        'ssm_centric':            'test_ssm_centric__',
        'ssm_occurrence_centric': 'test_ssm_occurrence_centric__'
    }

    input_dir = os.path.join(data_dir, 'input')
    output_dir = os.path.join(data_dir, 'output')
    maf_dir = os.path.join(input_dir, 'maf')

    # Old ones:
    # maf_urls = ['file://' + os.path.join(data_dir, 'kirp.mutect.test.maf'),
    #             'file://' + os.path.join(data_dir, 'kirp.muse.test.maf')]

    # Junjun's:
    maf_urls = ['file://' + os.path.join(maf_dir, f)
                for f in os.listdir(maf_dir) if f.split('.')[-1] == 'maf']

    cases_file = os.path.join(data_dir, 'cases.10429.json')
    citobands_file = os.path.join(exp_data_dir, 'genes.cytobands.tsv')
    census_file = os.path.join(exp_data_dir, 'cancer_gene_census_set.tsv')

    mappings = {'gene': 'gene.yml',
                'ssm': 'ssm.yml',
                'transcript': 'transcript.yml',
                'annotation': 'annotation.yml',
                'observation': 'observation.yml',
                }

    # maf_path = 'file:///test_mafs.csv'
    keep_indices = True
    maf_keep = False
    maf_use_existing = False


configs = {
    'BaseConfig': BaseConfig,
    'TestConfig': TestConfig
}
