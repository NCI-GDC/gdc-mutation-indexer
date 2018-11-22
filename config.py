import os
import uuid
import subprocess
import shlex
from elasticsearch import Elasticsearch
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

# from psqlgraph import PsqlGraphDriver
# from gdcdatamodel import models as md

from parsers import (
    Parser,
    S3Args,
    ESArgs,
    ESHadoopArgs,
    BuildArgs,
    SparkArgs,
)


ALL_PARSERS = [
    S3Args,
    ESArgs,
    ESHadoopArgs,
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


def get_release_info():
    """
    Lookup release candidate name and version in postgres
    """
    # postgres_driver = PsqlGraphDriver(
    #     os.environ["PG_HOST"],
    #     os.environ["PG_USER"],
    #     os.environ["PG_PASS"],
    #     os.environ["PG_NAME"],
    # )
    # with postgres_driver.session_scope():
    #     release_node = (postgres_driver.nodes(md.DataRelease)
    #                                    .props(released=False).first())
    # release_name = release_node.name
    # version = [release_node.major_version, release_node.minor_version]
    release_name = 'Marvin'
    version = [14, 0]
    return release_name, version


class BaseConfig(object):

    # Keywords that should appear in the S3 key for it to be picked up
    # Note that ALL of these keywords have to be present for the MAF to be used
    maf_keywords = ['SomaticMaf20170928', 'DR-10.0', 'somatic.maf.gz']  # NOTE: Will be removed when reading mafs from the index will be merged
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
        self.indices = self.get_index_names()
        self.maf_urls = self.get_maf_urls()
        self.gistic_urls = self.get_gistic_urls()

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
            for key, kwargs in parser.arguments.items():
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
            argparser = Parser.build([parser])
            args = argparser.parse_args(args)

            # Set properties with parsed values
            for key in parser.arguments:
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
        Get maf urls from s3 bucket

        TODO: Fetch relevant to the release urls from gdc_from_graph.file directly
        """
        bucket_contents = self.list_bucket(self.s3_maf_bucket)

        maf_urls = []
        for obj in bucket_contents:
            # If not all keywords present, skip
            if any([keyword not in obj.key for keyword in self.maf_keywords]):
                continue

            if any([pipeline in obj.key for pipeline in self.pipelines]):
                if any([project.replace('-', '.') in obj.key for project in self.projects]):
                    maf_urls.append(self.s3_maf_bucket + obj.key)

        return maf_urls

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
            if any([project.split('-')[1] in obj.key for project in self.projects]):
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


if __name__ == '__main__':
    conf = BaseConfig()
