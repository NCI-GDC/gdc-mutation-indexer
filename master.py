import subprocess
import logging
import os

from parsers import (
    Parser,
    SparkArgs,
)

from config import (
    VERSION,
    ROOT_DIR,
    LOG_FORMAT,
    ALL_PARSERS,
    get_git_commit,
)

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def parse_args():
    """
    Parse mutation indexer arguments
    """
    parser = Parser.build(
        ALL_PARSERS,
        description='Mutation Indexer',
    )

    args = parser.parse_args()
    args = process_args(args)
    Parser.log_args(args, ALL_PARSERS, logger)
    return args


def process_args(args):
    """
    Process parsed arguments
    Takes care of all argument dependencies and special treatment
    """
    # If source es creds not assigned, set them to ones of output es
    for key in ['host', 'port', 'user', 'pass']:
        param_name = 'source_es_{}'.format(key)
        if getattr(args, param_name) == '':
            value = getattr(args, 'es_{}'.format(key))
            setattr(args, param_name, value)

    return args


def get_spark_args(args):
    """
    Returns list of spark related command line arguments and values for `spark-submit`
    """
    # Get list of eggs and jars to upload
    jars_dir = os.path.join(ROOT_DIR, 'artifacts', 'jars')
    eggs_dir = os.path.join(ROOT_DIR, 'artifacts', 'eggs')
    jars = [os.path.join(jars_dir, j) for j in os.listdir(jars_dir)]
    eggs = [os.path.join(eggs_dir, e) for e in os.listdir(eggs_dir)]
    app_egg = 'gdc_mutation_indexer-{}_rev_{}-py2.7.egg'.format(VERSION,
                                                                get_git_commit(ROOT_DIR))
    eggs.append(os.path.join(ROOT_DIR, 'dist', app_egg))
    spark_args = ['--py-files', ','.join(eggs), '--jars', ','.join(jars)]

    # Add other spark arguments
    for arg in SparkArgs.arguments:
        name = '--' + arg
        value = str(getattr(args, arg.replace('-', '_')))
        spark_args.extend([name, value])
    return spark_args


def get_config_args(args):
    """
    Returns list of configuration arguments and values for `spark-submit`
    """
    config_args = []
    for parser in ALL_PARSERS:
        for name, info in parser.arguments.items():
            varname = name.upper().replace('-', '_')
            value = getattr(args, name.replace('-', '_'))
            if isinstance(value, list):
                value = ','.join(map(str, value))

            arg_action = info.get('action')
            # Do not pass bool flags if not needed
            # If default is True and value is True
            if arg_action == 'store_false' and value == 'True':
                continue
            # If default is False and value is False
            if arg_action == 'store_true' and value == 'False':
                continue
            config_args.extend(['--conf', 'spark.yarn.appMasterEnv.{}="{}"'.format(varname, value)])
            config_args.extend(['--conf', 'spark.executorEnv.{}="{}"'.format(varname, value)])

    return config_args


def get_submit_command(args):
    """
    Builds command to run to submit spark job
    """
    SPARK_HOME = os.getenv('SPARK_HOME')
    command = ['{}/bin/spark-submit'.format(SPARK_HOME)]
    command.extend(get_spark_args(args))
    command.extend(get_config_args(args))
    command.append(os.path.join(ROOT_DIR, 'bin/export.py'))
    return command


if __name__ == "__main__":
    args = parse_args()
    command = get_submit_command(args)
    subprocess.call(command)
