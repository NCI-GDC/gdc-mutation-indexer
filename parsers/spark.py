from base import BaseParser


class SparkArgs(BaseParser):
    """
    Spark submit arguments

    Specify settings that map directly to spark-submit command-line options
    (e.g., --num-executors).
    """

    @property
    def group(self):
        return {
            'title': 'Spark submit arguments',
            'description': 'Spark submit parameters',
        }

    @property
    def arguments(self):
        return {
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


class SparkConfArgs(BaseParser):
    """
    Spark configuration arguments

    Specify configuration options for the Spark context with periods
    replaced by hyphens. For example, --spark-sql-autoBroadcastJoinThreshold -1
    maps to --conf spark.sql.autoBroadcastJoinThreshold=-1.
    """

    @property
    def group(self):
        return {
            'title': 'Spark conf arguments',
            'description': 'Spark conf parameters',
        }

    @property
    def arguments(self):
        return {
            'spark-driver-maxResultSize': {
                'help': 'Maximum total size of results returned to driver',
                'default': '1g',
                'type': str,
            },
            'spark-sql-autoBroadcastJoinThreshold': {
                'help': 'Maximum broadcast join table size (-1 to disable)',
                'default': -1,
                'type': int,
            },
            'spark-sql-shuffle-partitions': {
                'help': 'Number of shuffle partitions for joins/aggregations',
                'default': 1024,
                'type': int,
            },
            'spark-executor-extraJavaOptions': {
                'help': 'A string of extra JVM options to pass to executors',
                'type': str,
            },
            'spark-network-timeout': {
                'help': 'Default timeout for all network interactions',
                'type': str,
            },
            'spark-executor-heartbeatInterval': {
                'help': "Interval between each executor's heartbeats to the driver",
                'type': str,
            },
            'spark-task-maxFailures': {
                'help': 'Number of failures of any particular task before giving up on the job',
                'type': str,
            },
            'spark-yarn-appMasterEnv-PYTHON-EGG-DIR': {
                'type': str,
            },
            'spark-yarn-appMasterEnv-PYTHON-EGG-CACHE': {
                'type': str,
            },
            'spark-executorEnv-PYTHON-EGG-DIR': {
                'type': str,
            },
            'spark-executorEnv-PYTHON-EGG-CACHE': {
                'type': str,
            },
            'spark-hadoop-fs-s3a-impl': {
                'type': str,
            },
            'spark-hadoop-fs-s3a-access-key': {
                'type': str,
            },
            'spark-hadoop-fs-s3a-secret-key': {
                'type': str,
            },
            'spark-hadoop-fs-s3a-endpoint': {
                'type': str,
            },
            'spark-hadoop-fs-s3a-connection-ssl-enabled': {
                'type': str,
            },
        }
