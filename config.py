import os

class BaseConfig(object):
    api_host = 'http://api.service.consul'
    signpost_host = 'http://signpost.service.consul'
    s3_host = 'http://cleversafe.service.consul'
    es_host = 'http://elasticsearchvis.service.consul'
    es_port = 9200

    # Source settings
    source_es_host = 'http://elasticsearch.service.consul'
    source_es_port = 9200
    graph_index = 'gdc_active_from_graph'


class TestConfig(BaseConfig):
    source_es_host = 'localhost'
    graph_index = 'test_graph_index__'

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests')
    data_dir = os.path.join(test_dir, 'data')
