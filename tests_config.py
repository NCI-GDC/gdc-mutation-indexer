import os
from config import BaseConfig


class TestConfig(BaseConfig):
    spark_master = 'local[1]'

    es_host = 'http://localhost'
    source_es_host = 'http://localhost'
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

    # maf_urls = ['file://' + os.path.join(maf_dir, f)
    #             for f in os.listdir(maf_dir) if f.endswith('maf')]

    maf_urls = ['file://' + os.path.join(maf_dir, 'sample-mutect.20170220')]

    # Additional test files
    #cases_file = os.path.join(input_dir, 'cases.8.json')
    cases_file = os.path.join(input_dir, 'cases.10429.json')
    case_mapping_json = os.path.join(input_dir, 'case_mapping.json')

    # Additional exports files
    # citobands_file = os.path.join(input_dir, 'genes.cytobands.tsv.gz')
    # census_file = os.path.join(input_dir, 'cancer_gene_census_set.tsv.gz')
    # gene_model_file = os.path.join(input_dir, 'genes.18.json.gz')

    keep_indices = True
    maf_keep = False
    maf_use_existing = False

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
    }
