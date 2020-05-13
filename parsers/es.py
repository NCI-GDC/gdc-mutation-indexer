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
            'es-nodes': {
                'help': (
                    'Comma-delimited list of Elasticsearch host:port pairs to write '
                    'to; e.g., "node1_ip:9200,node2_ip:9200"'
                ),
                'type': str,
                'default': 'localhost:9200',
            },
            'es-user': {
                'help': 'Elasticsearch user',
                'default': '',
            },
            'es-pass': {
                'help': 'Elasticsearch password',
                'default': '',
            },
            'es-use-ssl': {
                'help': 'HTTPS for Elasticsearch',
                'action': 'store_true',
                'default': False,
            },
            'disable-es-verify-certs': {
                'help': 'Verify the ca certs for Elasticsearch',
                'action': 'store_true',
                'default': False,
            },

            # Input Elasticsearch creds (if not provided, same as output ones)
            'source-es-nodes': {
                'help': 'host:port pairs for source Elasticsearch with graph indices.',
                'type': str,
                'default': '',
            },
            'source-es-user': {
                'help': 'Elasticsearch with graph indices user. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            'source-es-pass': {
                'help': 'Elasticsearch with graph indices password. '
                    'Keep empty if same as output elasticsearch.',
                'default': '',
            },
            'graph-case-index': {
                'help': 'Name of source Elasticsearch graph index with case data',
                'default': 'graph_case',
            },
            'graph-file-index': {
                'help': 'Name of source Elasticsearch graph index with file data',
                'default': 'graph_file',
            },
            'old-graph-index': {
                'help': (
                    'Name of Elasticsearch 5 graph index with case/file data. If set, '
                    'the individual graph/file indices are ignored.'
                ),
                'default': '',
            }
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
                'help': 'Number of partitions to distribute the index file across',
                'default': 2048,
                'type': int,
            },
            'df-coalesce': {
                'help': 'Decrease the number of partitions in the DataFrame to this number',
                'default': 12,
                'type': int,
            },
        }
