from base import BaseParser


class ESArgs(BaseParser):
    """
    Elasticsearch arguments
    """

    @property
    def group(self):
        return {
            'title': 'Elasticsearch arguments',
            'description': 'Elasticsearch related arguments',
        }

    @property
    def arguments(self):
        return {
            # Output Elasticsearch creds
            'es-host': {
                'help': 'Elasticsearch host',
                'default': 'http://localhost',
            },
            'es-port': {
                'help': 'Elasticsearch port',
                'default': 9200,
            },
            'es-user': {
                'help': 'Elasticsearch user',
                'default': '',
            },
            'es-pass': {
                'help': 'Elasticsearch password',
                'default': '',
            },
            'es-nodes': {
                'help': 'Coma-delimited dist of elasticsearch nodes to write to. '
                    'E.g. "node1_ip:9200,node2_ip:9200"',
                'type': str,
                'required': True,
            },
            # Input Elasticsearch creds (if not provided, same as output ones)
            'source-es-host': {
                'help': 'Elasticsearch with graph index host. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            'source-es-port': {
                'help': 'Elasticsearch with graph index port. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            'source-es-user': {
                'help': 'Elasticsearch with graph index user. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            'source-es-pass': {
                'help': 'Elasticsearch with graph index password. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            # Name of source graph index with case data
            'graph-index': {
                'help': 'Name of Elasticsearch graph index',
                'default': 'gdc_from_graph',
            },
        }


class ESHadoopArgs(BaseParser):
    """
    Elasticsearch-Hadoop adapter parameters
    """

    @property
    def group(self):
        return {
            'title': 'Elasticsearch-Hadoop adapter arguments',
            'description': 'Parameters to tweak ES Hadoop adapter',
        }

    @property
    def arguments(self):
        return {
            'batch-size-bytes': {
                'help': 'Batch size when writing DataFrame to Elasticsearch',
                'default': '16mb',
            },
            'batch-size-entries': {
                'help': 'Batch number of documents when writing DataFrame to Elasticsearch',
                'default': 1000,
                'type': int,
            },
            'df-repartition': {
                'help': 'Number of partitions to distribute the index file accross',
                'default': 2048,
                'type': int,
            },
            'df-coalesce': {
                'help': 'Decrease the number of partitions in the DataFrame to this number',
                'default': 12,
                'type': int,
            },
        }
