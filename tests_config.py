import os

from config import BaseConfig


class TestConfig(BaseConfig):
    # Directories used for test data
    root_dir = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(root_dir, 'tests')
    schemas_dir = os.path.join(root_dir, 'exports', 'schemas')
    data_dir = os.path.join(test_dir, 'data')
    log_dir = os.path.join(data_dir, 'log')
    input_dir = os.path.join(data_dir, 'input')
    maf_dir = os.path.join(input_dir, 'maf', 'merged_aliquot')
    gistic_dir = os.path.join(input_dir, 'cnv')

    # Initialize test directory tree if incomplete
    for directory in [log_dir, input_dir, maf_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Whether or not to rebuild graph index after every test
    graph_force_build = False

    # Whether or not to print document mismatches to stdout when testing
    print_data_errors = False

    # Whether or not to skip field-by-field data tests
    skip_in_depth_tests = True

    # Switch tests based on pruned/not_pruned version of indices
    indices_are_pruned = True

    # This grouping is useful to understand which tests to run
    main_indices = ['case_centric', 'gene_centric']
    ssm_indices = ['ssm_centric', 'ssm_occurrence_centric']
    cnv_indices = ['cnv_centric', 'cnv_occurrence_centric']

    # Additional test files
    doc_files = {
        'case': os.path.join(input_dir, 'cases.ndjson.gz'),
        'file': os.path.join(input_dir, 'files.ndjson.gz')
    }

    # Additional exports files
    citobands_file = os.path.join(input_dir, 'genes.cytobands.tsv.gz')
    census_file = os.path.join(input_dir, 'cancer_gene_census_set.tsv.gz')
    gene_model_file = os.path.join(input_dir, 'genes.ndjson.gz')

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
        'occurrences_per_cnv': 100,
    }

    cache_dataframes = {
         'mafs': True,
         'cases': False,
         'case_centric': True,
         'gene_centric': True,
         'ssm_centric': True,
         'ssm_occurrence_centric': True,
         'cnv_centric': True,
         'cnv_occurrence_centric': True,
     }

    def __init__(self):
        env = self.get_env_dict()
        super(TestConfig, self).__init__(env_dict=env)

        # To make it more convenient to write tests that load in data, put the names
        # of the graph indices in a dictionary keyed by doc type.
        self.graph_indices = {
            'case': self.graph_case_index, 'file': self.graph_file_index
        }

    def validate_indices(self, indices):
        existing_indices = self.es.indices.get_alias().keys()
        name_collisions = [name for name in indices.values()
                           if name in existing_indices]
        if name_collisions:
            for collision in name_collisions:
                self.es.indices.delete(collision)
            self.es.indices.refresh()

    @classmethod
    def get_env_dict(cls):
        """
        Simulate environment variables with dictionary
        """
        env_dict = {
            'BUILD_TYPE': 'develop',
            'ES_NODES': os.getenv('ES_NODES_TEST', 'http://localhost'),
            'ES_HOST': os.getenv('ES_HOST_TEST', 'http://localhost'),
            'ES_PORT': '9200',
            'SOURCE_ES_HOST': os.getenv('SOURCE_ES_HOST_TEST', 'http://localhost'),
            'SOURCE_ES_PORT': '9200',
            'S3_HOST': 'fake_s3',
            'S3_ACCESS_KEY': 'fake_s3_access',
            'S3_SECRET_KEY': 'fake_s3_secret',
            'DF_REPARTITION': '10',
            'INDEXD_USER': 'fake_indexd_user',
            'INDEXD_PASS': 'fake_indexd_pass',
        }

        return env_dict

    def get_maf_urls(self):
        return ['file://' + os.path.join(self.maf_dir, f)
                for f in os.listdir(self.maf_dir) if f.endswith('maf')]

    def get_gistic_urls(self):
        return ['file://' + os.path.join(self.gistic_dir, f)
                for f in os.listdir(self.gistic_dir)
                if f.endswith(".tsv")]
