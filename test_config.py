from config import BaseConfig


class TestConfig(BaseConfig):
    spark_master = 'local[1]'

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests')
    data_dir = os.path.join(test_dir, 'data')
    exp_data_dir = os.path.join(os.path.dirname(test_dir), 'exports', 'data')
    input_dir = os.path.join(data_dir, 'input')
    output_dir = os.path.join(data_dir, 'output')
    maf_dir = os.path.join(input_dir, 'maf')

    es_host = 'http://localhost'
    source_es_host = 'http://localhost'
    graph_index = 'test_graph_index__'

    # Whether or not to rebuild graph index after every test
    graph_force_build = False

    index_names = {
        'case_centric':         'test_case_centric__',
        'gene_centric':         'test_gene_centric__',
        'ssm_centric':          'test_ssm_centric__',
        'ssm_occurrence_centric':'test_ssm_occurrence_centric__'
    }

    # maf_urls = ['file://'+os.path.join(data_dir, 'kirp.mutect.test.maf'),
    #             'file://'+os.path.join(data_dir, 'kirp.muse.test.maf')]

    # Junjun's:
    maf_urls = ['file://' + os.path.join(maf_dir, f)
                for f in os.listdir(maf_dir) if f.endswith('maf')]

    cases_file = os.path.join(input_dir, 'cases.8.json')
    case_mapping_json = os.path.join(data_dir, 'case_mapping.json.gz')
    citobands_file = os.path.join(exp_data_dir, 'genes.cytobands.tsv.gz')
    census_file = os.path.join(exp_data_dir, 'cancer_gene_census_set.tsv.gz')
    gene_model_file = os.path.join(exp_data_dir, 'genes.18.json.gz')


    # maf_path = 'file:///test_mafs.csv'
    keep_indices = True
    maf_keep = False
    maf_use_existing = False
