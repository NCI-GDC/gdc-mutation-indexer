import os
import uuid
from elasticsearch import Elasticsearch
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

from exports.es_utils import (
    iterate_es_results,
    get_values_from_path,
)

from indexclient.client import IndexClient


class BaseConfig(object):

    # The Spark application name
    app_name = 'GDC_Mutation_Export'

    s3_host = 's3://{}'.format(os.getenv('S3_HOST', 'cleversafe.service.consul'))
    s3_access_key = os.getenv('S3_ACCESS_KEY', '')
    s3_secret_key = os.getenv('S3_SECRET_KEY', '')

    s3_maf_bucket = 's3a://{}/'.format(os.getenv('S3_MAF_BUCKET', 'somatic-maf'))
    s3_gistic_bucket = 's3a://{}/'.format(os.getenv('S3_GISTIC_BUCKET', 'gistic-cnv'))

    es_host = os.getenv('ES_HOST', 'http://localhost')
    es_port = os.getenv('ES_PORT', 9200)
    es_nodes = os.getenv('ES_NODES', '{}:{}'.format(es_host, es_port))
    es_user = os.getenv('ES_USER', '')
    es_pass = os.getenv('ES_PASS', '')

    # Indexd
    indexd = IndexClient(
        baseurl=os.getenv('INDEXD_HOST'),
        auth=(os.getenv('INDEXD_USER'), os.getenv('INDEXD_PASS'))
    )

    # Keywords that should appear in the S3 key for it to be picked up
    # Note that ALL of these keywords have to be present for the MAF to be used
    maf_keywords = os.getenv('MAF_KEYWORDS')
    maf_keywords = [keyword.strip() for keyword in maf_keywords.split(',')] if maf_keywords else []
    # maf_keywords = ['SomaticMaf20170510', 'DR-7.0', '.maf.gz']

    gistic_filename_string = os.getenv('GISTIC_FILENAME_STRING', 'focal_score_by_genes')

    # Pipelines to use. If an empty list is given, all 4 pipelies will be used
    # somaticsniper: 2227614  2.6GB
    # muse: 2730127  3.1GB
    # varscan: 2782495  3.2GB
    # mutect: 3416739  3.9GB
    pipelines = os.getenv('PIPELINES')
    pipelines = [pipeline.strip() for pipeline in pipelines.split(',')] if pipelines else []
    # pipelines = ['somaticsniper', 'mutect']

    # Projects to use. If an empty list is given, all 33 projects will be used
    projects = os.getenv('PROJECTS')
    projects = [project.strip() for project in projects.split(',')] if projects else []
    # projects = ['BLCA', 'BRCA']

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
        'ssm_occurrence_centric': 'ssm_occurrence_centric',
        'cnv_centric': 'cnv_centric',
        'cnv_occurrence_centric': 'cnv_occurrence_centric',
    }

    # Where to save each index's final json
    index_paths = {
        'case_centric': s3_maf_bucket + 'case-centric.json',
        'gene_centric': s3_maf_bucket + 'gene-centric.json',
        'ssm_centric': s3_maf_bucket + 'ssm-centric.json',
        'ssm_occurrence_centric': s3_maf_bucket + 'ssm-occurrence-centric.json'
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

    # Whether to save the maf, gistic files when done
    maf_keep = False
    gistic_keep = False
    # The location to save the combined maf and gistic dataframes
    maf_path = 'maf_df.parquet'
    gistic_path = 'gistic_df.parquet'
    # Use combined and saved maf, gistic files if they exist
    maf_use_existing = False
    gistic_use_existing = False
    # Whether to overwrite combined maf, gistic files
    maf_overwrite = True
    gistic_overwrite = True

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
        'occurrences_per_cnv': 100,
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
        'ssm_occurrence_centric': True,
        'cnv_centric': True,
        'cnv_occurrence_centric': True,
    }

    # Case load settings
    case_exclude_fields = [
        'project.disease_type',
        'project.primary_site',
        'case_autocomplete',
        'annotations',
        'days_to_index',
        'diagnoses.treatments',
        'tissue_source_site',
        'family_histories',
        'samples',
        'files',
        '*_ids'
    ]

    def __init__(self):
        self.es = Elasticsearch(
            self.es_host, port=self.es_port,
            http_auth=(self.es_user, self.es_pass)
        )
        self.indices = self.get_index_prefixes()
        self.maf_urls = self.get_maf_urls()
        self.gistic_urls = self.get_gistic_urls()

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
                    versions = (versions + [int(v.split('_')[1].replace('r', ''))
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

    def get_maf_urls_from_s3_bucket(self):
        """
        Get maf urls from s3 bucket

        TODO: Fetch relevant to the release urls from gdc_from_graph.file directly
        """
        bucket_contents = self.list_bucket(self.s3_maf_bucket)

        maf_urls = []
        for obj in bucket_contents:
            skip = False
            for keyword in self.maf_keywords:
                if keyword not in obj.key:
                    skip = True
                    break
            if not skip:
                if not self.pipelines or any([pipeline in obj.key for pipeline in self.pipelines]):
                    if not self.projects or any([project in obj.key for project in self.projects]):
                        maf_urls.append(self.s3_maf_bucket + obj.key)
                        if self.nb_projects and len(maf_urls) >= self.nb_projects:
                            break

        return maf_urls

    def get_maf_urls(self):

        all_mafs = self.get_file_ids_by_filename_regex(
            self.es, self.graph_index,
            "downstream_analyses.output_files.file_name",
            ".*maf.gz.?"
        )

        relevant_mafs = []
        for maf_name in all_mafs:
            if not self.projects or any([project in maf_name for project in self.projects]):
                maf_url = self.get_url_from_indexd(maf_name)
                relevant_mafs.append(maf_url)

        if self.nb_projects:
            relevant_mafs = relevant_mafs[:self.nb_projects]
        return relevant_mafs

    def get_url_from_indexd(self, file_name):
        """
        Queries indexd by file_name and returns corresponding validated cleversafe url
        """

        indexd_doc = self.indexd.list_with_params(
            params={'file_name': file_name}
        ).next()

        valid_metadata = {'type': 'cleversafe', 'state': 'validated'}
        for url, metadata in indexd_doc.urls_metadata.keys():
            if metadata == valid_metadata:
                return url

        raise Exception(
            'Did not find validated cleversafe url for {}'.format(file_name)
        )

    def get_maf_file_names(self):
        """
        The file name that corresponds to the File node
        in gdc_from_graph is the last part of the url.
            e.g. ['//filename/blah/blah2'] becomes ['blah2']
        """
        return [url.split('/')[-1] for url in self.maf_urls]

    def get_gistic_urls(self):
        """
        Get gistic urls from s3 bucket

        TODO: Fetch relevant to the release urls from gdc_from_graph.file directly
        """

        bucket_contents = self.list_bucket(self.s3_gistic_bucket)

        gistic_urls = []
        for obj in bucket_contents:
            if not self.projects or any([project in obj.key for project in self.projects]):
                #if 'all_thresholded.by_genes.txt' in obj.key:
                if self.gistic_filename_string in obj.key:
                    gistic_urls.append(self.s3_gistic_bucket + obj.key)

        return gistic_urls

    def list_bucket(self, bucket_name):
        """
        Return iterator over bucket contents
        """
        def get_bucket_name(url):
            """ Extract bucket name from bucket url """
            if url.endswith('/'):
                url = url[:-1]

            for prefix in ['s3://', 's3a://']:
                url = url.replace(prefix, '')

            return url

        conn = S3Connection(self.s3_access_key,
                            self.s3_secret_key,
                            host=self.s3_host.split('/')[-1],
                            calling_format=OrdinaryCallingFormat(),
                            is_secure=False)
        bucket = conn.get_bucket(get_bucket_name(bucket_name))
        return bucket.list()

    @staticmethod
    def get_file_ids_by_filename_regex(es, graph_index_name, path, regexp):
        """
        Returns all file ids from gdc_from_graph.file documents
        :path - dot-delimited path to file_name in file document
        :regexp - regular expression file_name field should follow (e.g. ".*maf.gz.?")
        """
        if not path.endswith('.file_name'):
            raise ValueError(
                'Unexpected path to file_name: {}'.format(path)
            )

        query = {
            "query": {
                "nested": {
                    "path": '.'.join(path.split('.')[:-1]),
                    "query": {
                        "regexp": {
                            path: regexp
                        }
                    }
                }
            },
            '_source': [path]
        }

        file_ids = set()
        for doc in iterate_es_results(es, graph_index_name, 'file', query=query):
            file_ids.update([doc['_id']])

        print file_ids
        print len(file_ids)
        return file_ids


if __name__ == '__main__':
    conf = BaseConfig()
