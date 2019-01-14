import os
import ssl
import sys
import uuid
import httplib
import subprocess
import shlex
from elasticsearch import Elasticsearch
from distutils.version import StrictVersion
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from exports.es_utils import iterate_es_results
from indexclient.client import IndexClient
from parsers import (
    ParserBuilder,
    S3Args,
    ESArgs,
    ESHadoopArgs,
    BuildArgs,
    SparkArgs,
    IndexdArgs,
)


def create_factory(host, port=443, timeout=10):
    return (
        httplib.HTTPSConnection(
            host=host,
            port=port,
            timeout=timeout,
            context=ssl._create_unverified_context()
        )
    )


py_ver = ".".join(str(sys.version_info[i]) for i in xrange(3))
if StrictVersion(py_ver) >= StrictVersion('2.7.9'):
    factory = (create_factory, ())
else:
    factory = None


ALL_PARSERS = [
    S3Args,
    ESArgs,
    ESHadoopArgs,
    IndexdArgs,
    BuildArgs,
    SparkArgs
]
LOG_FORMAT = '%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s'
CONFIG_PATH = os.path.abspath(__file__)
ROOT_DIR = os.path.dirname(CONFIG_PATH)

VERSION = "0.1.5"


def get_git_commit(git_dir):
    return subprocess.check_output(
        shlex.split('git --git-dir={}/.git rev-parse HEAD'.format(git_dir))
    ).strip()


class BaseConfig(object):

    # data_type field values that correspond to MAF files in gdc_from_graph.file
    maf_data_types = [
        'Aggregated Somatic Mutation',
        'Masked Somatic Mutation',
    ]
    gistic_filename_string = 'focal_score_by_genes'  # NOTE: this will be removed when gistics will be read from graph

    mappings = {
        'ssm': 'ssm.yml',
        'gene': 'gene.yml',
        'transcript': 'transcript.yml',
        'annotation': 'annotation.yml',
        'observation': 'observation.yml',
    }

    # Used for loading case/graph documents from a different es cluster
    graph_index = 'gdc_from_graph'
    graph_document = 'case'

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The location of the gene model json
    gene_model_file = 's3a://test/genes.hg38.v2.json'
    citobands_file = 's3a://test/genes.cytobands.tsv.gz'
    census_file = 's3a://test/cancer_gene_census_set.tsv.gz'

    # The location to save the combined maf and gistic dataframes
    maf_path = 'maf_df.parquet'
    gistic_path = 'gistic_df.parquet'

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
        'occurrences_per_cnv': 100,
    }

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

    def __init__(self, env_dict=None):
        """
        :env_dict<dict> - if set, will assign parameters from this dict instead of environment variables
        """
        self.assign_all_parameters(env_dict=env_dict)
        self.es = Elasticsearch(
            self.es_host, port=self.es_port,
            http_auth=(self.es_user, self.es_pass)
        )
        self.indexd = IndexClient(
            baseurl='{}:{}'.format(self.indexd_host, self.indexd_port),
            auth=(self.indexd_user, self.indexd_pass)
        )
        self.indices = self.get_index_names()
        self.maf_urls = self.get_maf_urls()
        self.maf_file_names = self.get_maf_file_names()
        self.gistic_urls = self.get_gistic_urls()
        self._acls = None

    def assign_all_parameters(self, env_dict=None):
        """
        Takes care of all config parameters to be set correctly
        Will assign self.var_name = value where os.environ['VAR-NAME'] == value
        for each parameter defined in ALL_PARSERS

        :env_dict<dict> - if set, will assign parameters from this dict instead of environment variables
        """
        if env_dict is None:
            env_dict = os.environ
        # Assign all arguments defined in parsers to corresponding values from env
        for parser in ALL_PARSERS:
            args = []
            # Gather arguments from env
            for key, kwargs in parser().arguments.items():
                arg_action = kwargs.get('action')
                is_arg_bool = arg_action in ['store_true', 'store_false']
                is_arg_list = kwargs.get('nargs') is not None
                # Get value from env
                value = env_dict.get(key.upper().replace('-', '_'))
                if value is None:
                    continue
                # Split lists and handle bools
                if is_arg_list:
                    values = value.split(',')
                else:
                    values = [value]

                # Add to arguments list:
                # Do not pass bool flags if not needed
                # Skip when default is False and value is False
                if arg_action == 'store_true' and value == 'False':
                    continue

                # Skip when default is True and value is True
                if arg_action == 'store_false' and value == 'True':
                    continue

                # Append argument
                args.append('--{}'.format(key))
                # Append values
                if not is_arg_bool:  # Bool args have no values
                    args.extend(values)

            # Build parser and parse gathered arguments
            argparser = ParserBuilder.build([parser])
            args = argparser.parse_args(args)

            # Set properties with parsed values
            for key in parser().arguments:
                key = key.replace('-', '_')
                value = getattr(args, key)
                setattr(self, key, value)

    def get_index_names(self):
        """
        Returns {index_type: es_index_name} dictionary
        """
        if self.build_type == 'release':
            prefix = 'release-'
        else:
            prefix = ''

        version_tag = '_'.join(map(str, self.build_version))
        indices = {
            index_type: prefix + '{}-{}-{}'.format(
                self.build_label, version_tag, index_type,
            )
            for index_type in self.index_types
        }

        existing_indices = self.es.indices.get_alias().keys()
        name_collisions = [name for name in indices.values()
                           if name in existing_indices]
        if name_collisions:
            raise Exception(
                "These indices already exist: {}.\n"
                "Change version or label, or remove existing indices"
                .format(', '.join(name_collisions))
            )
        return indices

    def get_raw_output_path(self, index_name):
        return self.s3_raw_bucket + index_name + '.json'

    def get_maf_urls(self):
        """
        Returns list of relevant maf_urls
        - gets maf file_id-s from elasticsearch "{self.graph_index}/file" index
        - gets corresponding urls from indexd
        """
        query = {
            "_source": ["file_name"],
            "query": {
                "terms": {
                    "data_type": self.maf_data_types
                }
            }
        }

        file_id_to_name = {}
        for doc in iterate_es_results(self.es, self.graph_index, 'file', query=query):
            file_id_to_name[doc['_id']] = doc['_source']['file_name']

        # Get urls from indexd for relevant files
        maf_urls = []
        for file_id, maf_name in file_id_to_name.items():
            if not self.projects or any([project.replace('-', '.') in maf_name for project in self.projects]):
                maf_url = self.get_url_from_indexd(file_id)
                # only add urls that are not protected
                # NOTE: this has to be removed once DAVE CA is properly implemented
                if 'protected.maf.gz' not in maf_url:
                    maf_urls.append(self.patch_s3_url(maf_url))

        return maf_urls

    def patch_s3_url(self, url):
        """
        Change s3 url to s3a
        """
        url = url.replace('s3://cleversafe.service.consul/', '')
        url = 's3a://' + url
        return url

    def get_url_from_indexd(self, file_id):
        """
        Queries indexd by file_name and returns corresponding validated cleversafe url
        """

        indexd_doc = self.indexd.get(file_id)

        valid_metadata = {'type': 'cleversafe', 'state': 'validated'}
        for url, metadata in indexd_doc.urls_metadata.items():
            if all([metadata.get(k) == v for k, v in valid_metadata.items()]):
                return url

        raise Exception(
            'Did not find validated cleversafe url for {}'.format(file_id)
        )

    def get_maf_file_names(self):
        """
        Transform the full url to the file_name stored in the File node
        """
        return [self.maf_url_to_file_name(url) for url in self.maf_urls]

    def maf_url_to_file_name(self, url):
        """
        Trim out leading folders;
        Mafs may be zipped or unzipped, but
        we expect the file_name in the File to be 'xxx.gz'
        """
        file_name = url.split('/')[-1]

        if not file_name.endswith('.gz'):
            file_name += '.gz'
        return file_name

    def get_gistic_urls(self):
        """
        Get gistic urls from s3 bucket

        TODO: Fetch relevant to the release urls from gdc_from_graph.file directly
        """

        bucket_contents = self.list_bucket(self.s3_gistic_bucket)
        gistic_urls = []
        for obj in bucket_contents:
            # Filter out files by gistic keyword string
            if not self.gistic_filename_string in obj.key:
                continue

            # Filter out irrelevant projects
            if self.projects != []:
                relevant_project = any([project.split('-')[1] in obj.key for project in self.projects])
                if not relevant_project:
                    continue

            # Add gistic url
            gistic_urls.append(self.s3_gistic_bucket + obj.key)

        return gistic_urls

    @property
    def acls(self):
        if self._acls is None:
            self._acls = self.get_acls()
        return self._acls

    def get_acls(self):
        """
        1. Take list of maf file names
        2. Assume the last part of the url is the file_name
        3. Look up corresponding files in es
        4. Parse out those files' acls
        """

        file_names = self.maf_file_names

        query = {
                "query": {
                    "bool": {
                        "must": {
                            "terms": {
                                "file_name": file_names
                                }
                            }
                        }
                    },
                "_source": ["file_name", "acl"],
                "size": 10000,
        }

        docs = self.es.search(index=self.graph_index,
                              doc_type='file',
                              body=query)

        # Build up dictionary of file_name to acl
        filenames_to_acls = {}
        for doc in docs['hits']['hits']:
            source = doc['_source']
            filename = source['file_name']
            acl = source['acl']

            filenames_to_acls[filename] = acl

        return filenames_to_acls

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
                            validate_certs=False,
                            https_connection_factory=factory,
                            calling_format=OrdinaryCallingFormat(),
                            is_secure=True)
        bucket = conn.get_bucket(get_bucket_name(bucket_name))
        return bucket.list()


if __name__ == '__main__':
    conf = BaseConfig()
