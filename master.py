import subprocess
import shlex
import logging
import os

from parsers import (
    Parser,
    S3Args,
    ESArgs,
    BuildArgs,
    SparkArgs,
)

from config import (
    VERSION,
    ROOT_DIR,
    LOG_FORMAT,
    ALL_PARSERS,
    GIT_HEAD_REV,
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
        description='Mutation indexer argument',
    )

    args = parser.parse_args()
    Parser.log_args(args, ALL_PARSERS, logger)
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
    app_egg = 'gdc_mutation_indexer-{}_rev_{}-py2.7.egg'.format(VERSION, GIT_HEAD_REV)
    eggs.append(os.path.join(ROOT_DIR, 'dist', app_egg))
    spark_args = ['--py-files', ','.join(eggs), '--jars', ','.join(jars)]

    # Add other spark arguments
    for arg in SparkArgs.args:
        name = '--' + arg.replace('_', '-')
        value = str(getattr(args, arg))
        spark_args.extend([name, value])
    return spark_args


def get_config_args(args):
    """
    Returns list of configuration arguments and values for `spark-submit` 
    """
    config_args = []
    for arg in S3Args.args | ESArgs.args | BuildArgs.args:
        varname = arg.upper()
        value = getattr(args, arg)
        if isinstance(value, list):
            if arg == 'version':
                separator = '_'
            else:
                separator = ','
            value = separator.join(map(str, value))
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
