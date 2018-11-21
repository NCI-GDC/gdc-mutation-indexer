from base import Parser


class SparkArgs(Parser):
    """
    Spark configuration arguments
    """
    group = {
        'title': 'Spark arguments',
        'description': 'Spark configuration parameters',
    }

    arguments = {
        'name': {
            'help': 'Name of the spark app',
            'default': 'GDC Mutation Indexer',
        },
        'master': {
            'help': 'Spark master',
            'default': 'yarn',
        },
        'deploy-mode': {
            'help': 'Spark deploy mode',
            'default': 'cluster',
        },
        'executor-memory': {
            'help': 'Spark executor memory',
            'default': '40g',
            'type': str,
        },
        'driver-memory': {
            'help': 'Spark driver memory',
            'default': '12g',
            'type': str,
        },
        'num-executors': {
            'help': 'The number of executors',
            'default': 25,
            'type': int,
        },
        'executor-cores': {
            'help': 'The number of executor CPU cores',
            'default': 8,
            'type': int,
        },
    }
