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
    output_dir = os.path.join(data_dir, 'output')
    maf_dir = os.path.join(input_dir, 'maf')

    # Initialize test directory tree if incomplete
    for directory in [log_dir, input_dir, output_dir, maf_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)


    spark_master = 'local[1]'

    es_host = 'http://localhost'
    source_es_host = 'http://localhost'
    s3_bucket = 'file:///'+os.path.abspath('tests/data/output/test_bucket')+'/'
    graph_index = 'test_graph_index__'

    # Whether or not to rebuild graph index after every test
    graph_force_build = True

    # Whether or not to print document mismatches to stdout when testing
    print_data_errors = False

    # Whether or not to skip field-by-field data tests
    skip_in_depth_tests = True

    index_names = {
        'case_centric': 'test_case_centric__',
        'gene_centric': 'test_gene_centric__',
        'ssm_centric': 'test_ssm_centric__',
        'ssm_occurrence_centric': 'test_ssm_occurrence_centric__'
    }
    # Where to save each index
    index_paths = {
        'case_centric':           s3_bucket+'test-case-centric.json',
        'gene_centric':           s3_bucket+'test-gene-centric.json',
        'ssm_centric':            s3_bucket+'test-ssm-centric.json',
        'ssm_occurrence_centric': s3_bucket+'test-ssm-occurrence-centric.json'
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
    cases_file = os.path.join(input_dir, 'cases.11.json')
    case_mapping_json = os.path.join(input_dir, 'case_mapping.json')

    # Additional exports files
    citobands_file = os.path.join(input_dir, 'genes.cytobands.tsv.gz')
    census_file = os.path.join(input_dir, 'cancer_gene_census_set.tsv.gz')
    gene_model_file = os.path.join(input_dir, 'genes.19.json.gz')

    keep_indices = True
    maf_keep = False
    maf_use_existing = False

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
    }

    def __init__(self):
        super(TestConfig, self).__init__()

    def get_maf_urls(self):
        return ['file://' + os.path.join(self.maf_dir, f)
                for f in os.listdir(self.maf_dir) if f.endswith('maf')]
