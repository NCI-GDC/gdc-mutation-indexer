import os
import uuid
from elasticsearch import Elasticsearch
from boto.s3.connection import S3Connection, OrdinaryCallingFormat


class BaseConfig(object):


    # The Spark application name
    app_name = 'GDC_Mutation_Export'

    s3_host = os.getenv('S3_HOST', 's3://cleversafe.service.consul')
    s3_bucket = 's3a://{}/'.format(os.getenv('S3_BUCKET', 'gdc-mafs'))
    s3_access_key = os.getenv('S3_ACCESS_KEY', '')
    s3_secret_key = os.getenv('S3_SECRET_KEY', '')
    s3_proxy = os.getenv('S3_PROXY', 'http://localhost')
    s3_proxy_port = os.getenv('S3_PROXY_PORT', 80)

    es_host = os.getenv('ES_HOST', 'http://localhost')
    es_port = os.getenv('ES_PORT', 9200)
    es_nodes = os.getenv('ES_NODES', '{}:{}'.format(es_host, es_port))
    es_user = os.getenv('ES_USER', '')
    es_pass = os.getenv('ES_PASS', '')


    # Keywords that should appear in the S3 key for it to be picked up
    # Note that ALL of these keywords have to be present for the MAF to be used
    maf_keywords = os.getenv('MAF_KEYWORDS')
    maf_keywords = [keyword.strip() for keyword in maf_keywords.split(',')] if maf_keywords else []
    #maf_keywords = ['SomaticMaf20170510', 'DR-7.0', '.maf.gz']

    # Pipelines to use. If an empty list is given, all 4 pipelies will be used
    # somaticsniper: 2227614  2.6GB
    # muse: 2730127  3.1GB
    # varscan: 2782495  3.2GB
    # mutect: 3416739  3.9GB
    pipelines = os.getenv('PIPELINES')
    pipelines = [pipeline.strip() for pipeline in pipelines.split(',')] if pipelines else []
    #pipelines = ['somaticsniper', 'mutect']

    
    # Projects to use. If an empty list is given, all 33 projects will be used
    projects = os.getenv('PROJECTS')
    projects = [project.strip() for project in projects.split(',')] if projects else []
    #projects = ['BLCA', 'BRCA']

    # Number of projects to use. Set to 0 to use all projects
    # The projects are taken in alphabetic order
    # To target specific projects, use 'projects' above
    nb_projects = os.getenv('NB_PROJECTS', 0)

    # Debug mode
    debug = False

    # Index names, these also double as document type names
    # If name is None, the index will not be built
    index_names = {
        'case_centric': 'case_centric',
        'gene_centric': 'gene_centric',
        'ssm_centric': 'ssm_centric',
        'ssm_occurrence_centric': 'ssm_occurrence_centric'
    }

    # Where to save each index's final json
    index_paths = {
        'case_centric': s3_bucket + 'case-centric.json',
        'gene_centric': s3_bucket + 'gene-centric.json',
        'ssm_centric': s3_bucket + 'ssm-centric.json',
        'ssm_occurrence_centric': s3_bucket + 'ssm-occurrence-centric.json'
    }
    # Whether to save the indices once they've been built
    index_keep = False
    # Load a prebuilt index and load it into elasticsearch
    index_use_existing = False
    # Whether to overwrite a built index file, if it exists
    index_overwrite = True

    mappings = {
                'ssm': 'ssm.yml',
                'gene': 'gene.yml',
                'transcript': 'transcript.yml',
                'annotation': 'annotation.yml',
                'observation': 'observation.yml',
                }

    # Index revision number, will be determined automatically if not specified
    revision = None

    # Used for loading case/graph documents from a different es cluster
    source_es_host = os.getenv('SOURCE_ES_HOST', es_host)
    source_es_port = os.getenv('SOURCE_ES_PORT', es_port)
    source_es_user = os.getenv('SOURCE_ES_USER', es_user)
    source_es_pass = os.getenv('SOURCE_ES_PASS', es_pass)
    graph_index = os.getenv('SOURCE_ES_INDEX', 'gdc_from_graph')
    graph_document = os.getenv('SOURCE_ES_DOCUMENT', 'case')



    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The location of the gene model json
    gene_model_file = 's3a://test/genes.hg38.v2.json'
    citobands_file = 's3a://test/genes.cytobands.tsv.gz'
    census_file = 's3a://test/cancer_gene_census_set.tsv.gz'


    # The location of the combined maf file
    maf_path = 's3a://test/uat_mafs.csv'
    # Whether to save the maf file or discard it when done
    maf_keep = False
    # Use combined maf if it already exists
    maf_use_existing = False
    # Whether to overwrite the combined maf file if it exists
    maf_overwrite = True

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
    }

    # How many partitions to distribute the index file accross
    # The index will be split up into this many json files
    repartition = 2048
    coalesce = 6
    batch_size_bytes = '3mb'
    batch_size_entries = '100'
    cache_dataframes = {
        'mafs': True,
        'cases': True,
        'case_centric': True,
        'gene_centric': True,
        'ssm_centric': True,
        'ssm_occurrence_centric':True
    }


    # Case load settings
    case_exclude_fields = ','.join(['project.disease_type',
                                    'project.primary_site',
                                    'samples',
                                    'annotations',
                                    'days_to_index',
                                    'summary.file_size',
                                    'summary.file_count',
                                    'summary.experimental_strategies',
                                    'diagnoses.treatments',
                                    'tissue_source_site',
                                    'exposures',
                                    'family_histories',
                                    'files',
                                    '*_ids'])


    def __init__(self):
        self.indices = self.get_index_prefixes()
        self.maf_urls = self.get_maf_urls()



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

        def get_indices_max_version():
            versions = []
            es = Elasticsearch(self.es_host,
                               port=self.es_port,
                               http_auth=(self.es_user, self.es_pass))

            indices = es.indices.get_alias().keys()

            for index_name in self.index_names.values():
                if index_name is not None:
                    versions = (versions + [int(v.split('_')[1].replace('r',''))
                                for v in indices if v.endswith(index_name)
                                            and v[:4] == 'gdc_'])

            if versions == []:
                version = 0
            else:
                version = max(versions) + 1
            return version

        def get_prefix(index_name):
            version = get_indices_max_version()
            prefix = 'gdc_r{}_{}'.format(version, index_name)
            return prefix

        indices = {k: get_prefix(v) for k, v in self.index_names.items()
                   if v is not None}
        return indices

    def get_maf_urls(self):
        conn = S3Connection(self.s3_access_key,
                            self.s3_secret_key,
                            host=self.s3_host.split('/')[-1],
                            proxy=self.s3_proxy,
                            proxy_port=self.s3_proxy_port,
                            calling_format=OrdinaryCallingFormat(),
                            is_secure=False)
        bucket_name = self.s3_bucket.split('/')[2]
        bucket = conn.get_bucket(bucket_name)

        maf_urls = []

        for obj in bucket.list():
            skip = False
            for keyword in self.maf_keywords:
                if not keyword in obj.key:
                    skip = True
                    break
            if not skip:
                if not self.pipelines or any([pipeline in obj.key for pipeline in self.pipelines]):
                    if not self.projects or any([project in obj.key for project in self.projects]):
                        maf_urls.append(self.s3_bucket + obj.key)
                        if self.nb_projects and len(maf_urls) >= self.nb_projects:
                            break

        return maf_urls

