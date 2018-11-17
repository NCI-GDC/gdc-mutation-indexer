from base import BaseArgs


class ESArgs(BaseArgs):
    """
    Elasticsearch arguments
    """
    args = {
        'es_host',
        'es_port',
        'es_user',
        'es_pass',
        'es_nodes',
        'es_batch_size_bytes',
        'es_batch_size_entries',
        'index_coalesce',
        'index_repartition',
        'source_es_host',
        'source_es_port',
        'source_es_user',
        'source_es_pass',
    }

    def add_args(self, parser):
        es_args = parser.add_argument_group(
            title='Elasticsearch arguments',
            description='Elasticsearch related arguments'
        )
        # Output Elasticsearch creds
        es_args.add_argument(
            '--es-host', help='Elasticsearch host',
            required=True,
        )
        es_args.add_argument(
            '--es-port', help='Elasticsearch port',
            default=9200,
        )
        es_args.add_argument(
            '--es-user', help='Elasticsearch user',
            required=True,
        )
        es_args.add_argument(
            '--es-pass', help='Elasticsearch password',
            required=True,
        )
        es_args.add_argument(
            '--es-nodes', help='List of elasticsearch nodes to write to',
            nargs='*',
            required=True,
        )
        # Input Elasticsearch creds (if not provided, same as output ones)
        es_args.add_argument(
            '--source-es-host', help='Elasticsearch with graph index host. '
            'Keep empty if same as output elasticsearch.',
            default=None,
        )
        es_args.add_argument(
            '--source-es-port', help='Elasticsearch with graph index port. '
            'Keep empty if same as output elasticsearch.',
            default=None,
        )
        es_args.add_argument(
            '--source-es-user', help='Elasticsearch with graph index user. '
            'Keep empty if same as output elasticsearch.',
            default=None,
        )
        es_args.add_argument(
            '--source-es-pass', help='Elasticsearch with graph index password. '
            'Keep empty if same as output elasticsearch.',
            default=None,
        )
        # Elasticsearch tweaking parameters
        es_hadoop_args = parser.add_argument_group(
            title='Elasticsearch-Hadoop adapter arguments',
            description='Parameters to tweak ES Hadoop adapter'
        )
        es_hadoop_args.add_argument(
            '--es-batch-size-bytes', help='', # FIXME: text
            default='16mb',
        )
        es_hadoop_args.add_argument(
            '--es-batch-size-entries', help='', # FIXME: text
            default=1000,
        )
        es_hadoop_args.add_argument(
            '--index-repartition', help='Number of partitions to distribute the index file accross',
            default=2048,
        )
        es_hadoop_args.add_argument(
            '--index-coalesce', help='', # FIXME: text
            default=12,
        )
        return parser
