import os
from config import BaseConfig, ReadWriteMode


class TestConfig(BaseConfig):
    # Directories used for test data
    root_dir = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(root_dir, 'tests')
    schemas_dir = os.path.join(root_dir, 'exports', 'schemas')
    data_dir = os.path.join(test_dir, 'data')
    log_dir = os.path.join(data_dir, 'log')
    input_dir = os.path.join(data_dir, 'input')
    maf_dir = os.path.join(input_dir, 'maf')
    gistic_dir = os.path.join(input_dir, 'cnv')

    # Initialize test directory tree if incomplete
    for directory in [log_dir, input_dir, maf_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    spark_master = 'local[1]'

    es_host = 'http://localhost'
    source_es_host = 'http://localhost'
    s3_maf_bucket = 'file:///' + os.path.abspath('tests/data/output/test_bucket') + '/'
    graph_index = 'test_graph_index__'

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

    # Where to save each index
    index_paths = {
        'case_centric': s3_maf_bucket + 'test-case-centric.json',
        'gene_centric': s3_maf_bucket + 'test-gene-centric.json',
        'ssm_centric': s3_maf_bucket + 'test-ssm-centric.json',
        'ssm_occurrence_centric': s3_maf_bucket + 'test-ssm-occurrence-centric.json'
    }
    # Whether to save the indices once they've been built
    index_keep = False
    # Load a prebuilt index and load it into elasticsearch
    index_use_existing = False
    # Whether to overwrite a built index file, if it exists
    index_overwrite = True
    # How many partitions to distribute the index file accross
    repartition = 10

    # Additional test files
    doc_files = {
        'case': os.path.join(input_dir, 'cases.json.gz'),
        'file': os.path.join(input_dir, 'files.json.gz')
    }

    # Additional exports files
    citobands_file = os.path.join(input_dir, 'genes.cytobands.tsv.gz')
    census_file = os.path.join(input_dir, 'cancer_gene_census_set.tsv.gz')
    gene_model_file = os.path.join(input_dir, 'genes.json.gz')

    # Whether to read/write/neither
    read_write_mode = {'maf': ReadWriteMode.neither,
                       'gistic': ReadWriteMode.neither}

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
        super(TestConfig, self).__init__()
        self.gistic_urls = self.get_gistic_urls()

    def get_maf_urls(self):
        return ['file://' + os.path.join(self.maf_dir, f)
                for f in os.listdir(self.maf_dir) if f.endswith('maf')]

    def get_gistic_urls(self):
        return ['file://' + os.path.join(self.gistic_dir, f)
                for f in os.listdir(self.gistic_dir)
                if f.endswith(".tsv")]
