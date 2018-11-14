from base import BaseArgs


class SparkArgs(BaseArgs):
    """
    Spark configuration arguments
    """
    args = {
        'name',
        'master',
        'deploy_mode',
        'executor_memory',
        'driver_memory',
        'executor_cores',
        'num_executors',
    }

    def add_args(self, parser):
        spark_args = parser.add_argument_group(
            title='Spark arguments',
            description='Spark configuration parameters'
        )
        spark_args.add_argument(
            '--name', help='Name of the spark app',
            default='GDC Mutation Indexer',
        )
        spark_args.add_argument(
            '--master', help='Spark master',
            default='yarn',
        )
        spark_args.add_argument(
            '--deploy-mode', help='Spark deploy mode',
            default='cluster',
        )
        spark_args.add_argument(
            '--executor-memory', help='Spark executor memory',
            default='40g', type=str,
        )
        spark_args.add_argument(
            '--driver-memory', help='Spark driver memory',
            default='12g', type=str,
        )
        spark_args.add_argument(
            '--num-executors', help='The number of executors',
            default=25, type=int,
        )
        spark_args.add_argument(
            '--executor-cores', help='The number of executor CPU cores',
            default=8, type=int,
        )
        return parser
