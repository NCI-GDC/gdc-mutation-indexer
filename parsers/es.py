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
    }

    def add_args(self, parser):
        es_args = parser.add_argument_group(
            title='Elasticsearch arguments',
            description='Elasticsearch related arguments'
        )
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
        es_args.add_argument(
            '--es-batch-size-bytes', help='Elasticsearch password',
            default='16mb',
        )
        es_args.add_argument(
            '--es-batch-size-entries', help='Elasticsearch password',
            default=1000,
        )

        return parser
