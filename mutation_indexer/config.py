import http.client
import os
import ssl
import sys
import uuid

import elasticsearch
import pkg_resources
from boto.s3 import connection
from elasticsearch import helpers
from indexclient import client

from mutation_indexer import parsers


def create_factory(host, port=443, timeout=10):
    """Create an HTTPSConnection factory that doesn't try to verify the server cert.

    Make it possible to connect to Cleversafe even though the Cleversafe cert doesn't
    match the hostname we likely expect.
    """
    return http.client.HTTPSConnection(
        host=host, port=port, timeout=timeout, context=ssl._create_unverified_context()
    )


factory = (create_factory, ())

ALL_PARSERS = [
    parsers.S3Args,
    parsers.ESArgs,
    parsers.ESHadoopArgs,
    parsers.IndexdArgs,
    parsers.BuildArgs,
    parsers.SparkArgs,
    parsers.SparkConfArgs,
]
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s"
CONFIG_PATH = os.path.abspath(__file__)
ROOT_DIR = os.path.dirname(CONFIG_PATH)

VERSION = pkg_resources.get_distribution(
    "gdc_mutation_indexer"
).version  # format: <version>.rev.<hash>
PYTHON_VERSION = ".".join(str(i) for i in sys.version_info[:2])


class BaseConfig(object):

    # data_type field values that correspond to MAF files in gdc_from_graph.file
    maf_data_types = [
        "Aggregated Somatic Mutation",
        "Masked Somatic Mutation",
    ]

    # Filename substring for identifying the specific MAFs we should index.
    # We only want the MAFs formatted for mutation indexer consumption.
    formatted_maf_keywords = "DR-10.0.somatic.maf.gz"

    # Filename substring for protected MAFs; i.e., MAFs potentially containing
    # germline data. We do _not_ want to index these due to privacy concerns.
    protected_maf_keywords = "protected.maf.gz"

    # Filename substring for identifying GISTIC files in the GISTIC bucket.
    # NOTE: This may be removed if we get GISTIC files from the graph instead.
    gistic_filename_string = "focal_score_by_genes"

    mappings = {
        "ssm": "ssm.yml",
        "gene": "gene.yml",
        "transcript": "transcript.yml",
        "annotation": "annotation.yml",
        "observation": "observation.yml",
    }

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID("d15296a3-38ed-412e-8ace-75e235f82f55")

    gencode_version = "v22"

    # The location of the gene model json
    gene_model_file = "s3a://gdc-mutation-indexer/genes.hg38.v2.json"
    citobands_file = "s3a://gdc-mutation-indexer/genes.cytobands.tsv.gz"
    census_file = "s3a://gdc-mutation-indexer/cancer_gene_census_set.tsv.gz"

    # The location to save the combined maf and gistic dataframes
    maf_path = "maf_df.parquet"
    gistic_path = "gistic_df.parquet"
    aliquot_path = "aliquot_df.parquet"
    gene_expression_values_path = "gene_expression_values_df.parquet"
    gene_expression_cases_path = "gene_expression_cases_df.parquet"
    primary_aliquot_path = "primary_aliquot_df.parquet"
    ascat_path = "ascat_df.parquet"
    gene_model_path = "gene_model_df.parquet"

    percentile_threshold = {
        "genes_per_case": 100,
        "occurrences_per_ssm": 100,
        "consequences_per_ssm": 100,
        "observations_per_ssm": 100,
        "occurrences_per_cnv": 100,
    }

    cache_dataframes = {
        "mafs": True,
        "cases": True,
        "case_centric": True,
        "gene_centric": True,
        "ssm_centric": True,
        "ssm_occurrence_centric": True,
        "cnv_centric": True,
        "cnv_occurrence_centric": True,
    }

    # Case load settings
    case_exclude_fields = [
        # Pieces of the graph index we don't want to copy over
        "annotations",
        "case_autocomplete",
        "family_histories",
        "files",
        "follow_ups",
        "project.disease_type",
        "project.primary_site",
        "*_ids",
        # Fields omitted from *_centric models that have values in graph index
        "diagnoses.annotations",
        "*.updated_datetime",
        "*.created_datetime",
        "project.releasable",
        "project.released",
        "project.state",
    ]

    samples_include_fields = ["samples.sample_type"]

    def __init__(self, env_dict=None):
        """
        :env_dict<dict> - if set, will assign parameters from this dict instead of environment variables
        """
        self.assign_all_parameters(env_dict=env_dict)

        # If we're configured to read from ES 5, configure the old doc types.
        # For ES 7, assume doc types don't exist.
        if self.old_graph_index:
            self.graph_case_index = self.old_graph_index
            self.graph_file_index = self.old_graph_index
            self.graph_case_doc_type = "case"
            self.graph_file_doc_type = "file"
        else:
            self.graph_case_doc_type = None
            self.graph_file_doc_type = None

        # If source es creds not assigned, set them to ones of output es
        for key in ["nodes", "user", "pass"]:
            param_name = f"source_es_{key}"
            if getattr(self, param_name) == "":
                value = getattr(self, f"es_{key}")
                setattr(self, param_name, value)

        # aliquot should be synced with maf, don't allow users to deviate
        self.aliquot_backup = self.maf_backup

        self.es = elasticsearch.Elasticsearch(
            self.es_nodes.split(","),
            use_ssl=self.es_use_ssl,
            verify_certs=not self.disable_es_verify_certs,
            http_auth=(self.es_user, self.es_pass),
        )

        self.source_es = elasticsearch.Elasticsearch(
            self.source_es_nodes.split(","),
            use_ssl=self.es_use_ssl,
            verify_certs=not self.disable_es_verify_certs,
            http_auth=(self.source_es_user, self.source_es_pass),
        )

        self.indexd = client.IndexClient(
            baseurl=f"{self.indexd_host}:{self.indexd_port}",
            auth=(self.indexd_user, self.indexd_pass),
        )

        self.indices = self.get_index_names()
        self.maf_urls = self.get_maf_urls()
        self.maf_file_names = self.get_maf_file_names()
        self.gistic_urls = self.get_gistic_urls()
        self._exclude_fields = None

        if self.blacklist_fields:
            self.exclude_fields.extend(self.blacklist_fields)

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
                arg_action = kwargs.get("action")
                is_arg_bool = arg_action in ["store_true", "store_false"]
                is_arg_list = kwargs.get("nargs") is not None
                # Get value from env
                value = env_dict.get(key.upper().replace("-", "_"))
                if value is None:
                    continue
                # Split lists and handle bools
                if is_arg_list:
                    values = value.split(",") if value else []
                else:
                    values = [value]

                # Add to arguments list:
                # Do not pass bool flags if not needed
                # Skip when default is False and value is False
                if arg_action == "store_true" and value == "False":
                    continue

                # Skip when default is True and value is True
                if arg_action == "store_false" and value == "True":
                    continue

                # Append argument
                args.append(f"--{key}")
                # Append values
                if not is_arg_bool:  # Bool args have no values
                    args.extend(values)

            # Build parser and parse gathered arguments
            argparser = parsers.ParserBuilder.build([parser])
            args = argparser.parse_args(args)

            # Set properties with parsed values
            for key in parser().arguments:
                key = key.replace("-", "_")
                value = getattr(args, key)
                setattr(self, key, value)

        if self.gencode_version == "v36":
            self.gene_model_file = (
                "s3a://gdc-mutation-indexer/genes.hg38.ensembl102.gencode36.json"
            )
            self.citobands_file = "s3a://gdc-mutation-indexer/gencode.v36.cytoband.par_removed.formated.tsv.gz"
            self.census_file = (
                "s3a://gdc-mutation-indexer/cancer_gene_census_set.gencode_v36.tsv.gz"
            )

    def get_index_names(self):
        """Create {index_type: es_index_name} dictionary based on build config."""
        if "__" in self.build_label:
            raise ValueError(
                f"Double underscores not allowed in build label {self.build_label}"
            )

        if self.study_label:
            if "__" in self.study_label:
                raise ValueError(
                    f"Double underscores not allowed in study label {self.study_label}"
                )

            template = f"{self.build_label}__{{}}__{self.study_label}__controlled"
        else:
            template = f"{self.build_label}__{{}}"

        indices = {
            index_type: template.format(index_type) for index_type in self.index_types
        }

        self.validate_indices(indices)

        return indices

    def validate_indices(self, indices):
        name_collisions = self.es.indices.get_alias().keys() & indices.values()

        if name_collisions:
            raise Exception(
                "These indices already exist: {}.\n"
                "Change version or label, or remove existing indices".format(
                    ", ".join(name_collisions)
                )
            )

    def get_raw_output_path(self, index_name):
        return self.s3_raw_bucket + index_name + ".json"

    def get_maf_urls(self):
        """
        Returns list of relevant maf_urls
        - gets maf file_id-s from elasticsearch "{self.graph_file_index}" index
        - gets corresponding urls from indexd
        """
        if self.skip_es_mafs:
            return self.include_maf_urls

        query = {
            "_source": ["file_name"],
            "query": {"terms": {"data_type": self.maf_data_types}},
        }

        es_results = helpers.scan(
            self.source_es,
            index=self.graph_file_index,
            doc_type=self.graph_file_doc_type,
            scroll="2m",
            size=100,
            query=query,
        )

        file_id_to_name = {
            doc["_id"]: doc["_source"]["file_name"] for doc in es_results
        }

        # Get urls from indexd for relevant files
        maf_urls = self.include_maf_urls + []
        for file_id, maf_name in file_id_to_name.items():
            if self.maf_passes_project_check(maf_name):
                maf_url = self.get_url_from_indexd(file_id)
                # NOTE: TEMP
                if self.temp_filter_maf_urls(maf_url):
                    maf_urls.append(self.patch_s3_url(maf_url))

        return list(set(maf_urls))

    def projects_valid(self):
        """
        Valid projects values: ['TCGA-UVM'], ['TCGA-UVM', 'FM-AD']
        Invalid projects values: None, [], ['']
        """
        return self.projects and any(self.projects)

    def maf_passes_project_check(self, maf_name):
        """
        If projects is a valid list, e.g. ['TCGA-UVM', 'FM-AD'],
            check that maf_name contains the equivalent phrase,
            'TCGA.UVM' or 'FM.AD'
        If projects is not valid, e.g. None, [], or [''],
            (due to unpacking spark variables on minion nodes)
        we do not need to filter. Maf passes check vacuously
        """
        if self.projects_valid():
            return any(
                [project.replace("-", ".") in maf_name for project in self.projects]
            )
        else:
            return True

    def gistic_passes_project_check(self, gistic_name):
        """
        If projects is a valid list, e.g. ['TCGA-UVM', 'FM-AD'],
            check that gistic_name contains the equivalent phrase
        If projects is not valid, e.g. None, [], or [''],
            (due to unpacking spark variables on minion nodes)
        we do not need to filter. Gistic passes check vacuously
        """
        if self.projects_valid():
            return any(
                [project.split("-")[1] in gistic_name for project in self.projects]
            )
        else:
            return True

    def temp_filter_maf_urls(self, maf_url):
        """
        Only add urls that are not protected
        and filter out improperly formatted mafs.
        NOTE: this has to be removed once DAVE CA is properly implemented
        TODO: Long-term solution for improperly formatted mafs
        """
        return (
            self.protected_maf_keywords not in maf_url
            and self.formatted_maf_keywords in maf_url
        )

    def patch_s3_url(self, url):
        """
        Change s3 url to s3a
        """
        url = url.replace("s3://cleversafe.service.consul/", "")
        url = "s3a://" + url
        return url

    def get_url_from_indexd(self, file_id):
        """
        Queries indexd by file_name and returns corresponding validated cleversafe url
        """

        indexd_doc = self.indexd.get(file_id)

        valid_metadata = {"type": "cleversafe", "state": "validated"}
        for url, metadata in indexd_doc.urls_metadata.items():
            if all(metadata.get(k) == v for k, v in valid_metadata.items()):
                return url

        raise Exception(f"Did not find validated cleversafe url for {file_id}")

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
        file_name = url.split("/")[-1]

        if not file_name.endswith(".gz"):
            file_name += ".gz"
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
            if self.gistic_filename_string not in obj.key:
                continue

            # Filter out irrelevant projects
            if self.gistic_passes_project_check(obj.key):
                # Add gistic url
                gistic_urls.append(self.s3_gistic_bucket + obj.key)

        return gistic_urls

    def get_samples_fields_to_exclude(self):
        """
        Gets the case.samples mapping from es and list all the first degree
        child fields (for example `samples.portions.analytes.annotations.entity_id`
        would become `samples.portions`) so they can be excluded. It will
        keep any field in `samples_include_fields`
        """
        samples_mapping = self.source_es.indices.get_field_mapping(
            index=self.graph_case_index,
            doc_type=self.graph_case_doc_type,
            fields="samples.*",
        )

        index_name = tuple(samples_mapping.keys())[0]
        if self.graph_case_doc_type:
            mapping = samples_mapping[index_name]["mappings"][self.graph_case_doc_type]
        else:
            mapping = samples_mapping[index_name]["mappings"]

        fields = mapping.keys()
        fields_to_exclude = {".".join(x.split(".", 2)[:2]) for x in fields}

        return list(fields_to_exclude - set(self.samples_include_fields))

    def list_bucket(self, bucket_name):
        """
        Return iterator over bucket contents
        """

        def get_bucket_name(url):
            """Extract bucket name from bucket url"""
            if url.endswith("/"):
                url = url[:-1]

            for prefix in ["s3://", "s3a://"]:
                url = url.replace(prefix, "")

            return url

        conn = connection.S3Connection(
            self.s3_access_key,
            self.s3_secret_key,
            host=self.s3_host.split("/")[-1],
            validate_certs=False,
            https_connection_factory=factory,
            calling_format=connection.OrdinaryCallingFormat(),
            is_secure=True,
        )
        bucket = conn.get_bucket(get_bucket_name(bucket_name))
        return bucket.list()

    @property
    def exclude_fields(self):
        if self._exclude_fields is None:
            self._exclude_fields = (
                self.case_exclude_fields + self.get_samples_fields_to_exclude()
            )
        return self._exclude_fields


if __name__ == "__main__":
    conf = BaseConfig()
