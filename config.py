import os
import uuid


class BaseConfig(object):
    # The Spark application name
    app_name = 'GDC_Mutation_Export'

    api_host = 'http://api.service.consul'
    signpost_host = 'http://signpost.service.consul'
    s3_host = 'http://cleversafe.service.consul'
    # This is the cluster where document will be loaded into
    es_host = 'http://elasticsearchvis.service.consul'
    es_port = 9200

    # Index names, these also double as document type names
    # If name is None, the index will not be built
    index_names = {
        'case_centric':         'case_centric',
        'gene_centric':         'gene_centric',
        'ssm_centric':          'ssm_centric',
        'ssm_ocurrence_centric':'ssm_occurrence_centric'
    }

    # Index revision number, will be determined automatically if not specified
    revision = None

    # Used for loading case/graph documents from a different es cluster
    source_es_host = 'http://elasticsearch.service.consul'
    source_es_port = 9200
    graph_document = 'case'
    graph_index = 'gdc_from_graph'

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The name of the combined maf file
    maf_path = 's3a://test/combined_mafs.csv'
    # Whether to save the maf file or discard it when done
    maf_keep = True
    # Use combined maf if it already exists
    maf_use_existing = True
    # Whether to overwrite the combined maf file if it exists
    maf_overwrite = True

    # Case load settings
    case_fields = 'case_id,submitter_id,state,project.*,program.*'
    case_arrays = ''

class TestConfig(BaseConfig):
    es_host = 'localhost'
    source_es_host = 'localhost'
    graph_index = 'test_graph_index__'

    index_names = {
        'case_centric':         'test_case_centric__',
        'gene_centric':         'test_gene_centric__',
        'ssm_centric':          'test_ssm_centric__',
        'ssm_ocurrence_centric':'test_ssm_occurrence_centric__'
    }

    keep_indices = False

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests')
    data_dir = os.path.join(test_dir, 'data')


configs = {
    'BaseConfig': BaseConfig,
    'TestConfig': TestConfig
}
