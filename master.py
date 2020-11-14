import logging
import os
import pprint
import subprocess
from functools import partial

from psqlgraph import PsqlGraphDriver
from gdcdatamodel import models as md
from gdcmodels import esutils

from parsers import (
    ParserBuilder,
    SparkArgs,
    SparkConfArgs,
)

from config import (
    VERSION,
    ROOT_DIR,
    LOG_FORMAT,
    ALL_PARSERS,
    get_git_commit,
    BaseConfig,
)
from exports.es_utils import get_non_null_fields


logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def get_release_info():
    """
    Lookup release candidate name and version in postgres
    """
    postgres_driver = PsqlGraphDriver(
        os.environ["PG_HOST"],
        os.environ["PG_USER"],
        os.environ["PG_PASS"],
        os.environ["PG_NAME"],
    )
    with postgres_driver.session_scope():
        release_node = (
            postgres_driver.nodes(md.DataRelease).props(released=False).first()
        )
    release_name = release_node.name
    version = [release_node.major_version, release_node.minor_version]
    return release_name, version


def parse_args():
    """
    Parse mutation indexer arguments
    """
    parser = ParserBuilder.build(ALL_PARSERS, description="Mutation Indexer",)
    args = parser.parse_args()
    return args


def no_op():
    """
    Callable that does nothing
    """
    pass


def raise_on_decline():
    """
    Raise a generic exception
    """
    raise Exception("User refused to continue")


def user_confirm(prompt_string, log, on_confirm=no_op, on_decline=raise_on_decline):
    """
    Prompt user confirmation to proceed

    :param prompt_string: prompt string
    :param log: logging instance
    :param on_confirm: callable to invoke when user confirms
    :param on_decline: callable to invoke when user declines
    """
    while True:
        log.info(prompt_string)
        ans = input().lower()
        if ans in ["y", "yes"]:
            return on_confirm()
        elif ans in ["n", "no"]:
            return on_decline()
        else:
            log.error("Invalid answer: {}".format(ans))


def confirm_args(args):
    """
    Confirm with user that args and index names are as expected
    """
    # Log arguments
    ParserBuilder.log_args(args, ALL_PARSERS, logger)

    # Initialize config with environment variables (the way spark worker will see it)
    env_dict = ParserBuilder.get_environment_dict(args, ALL_PARSERS)
    config = BaseConfig(env_dict=env_dict)

    # Confirm with user
    user_confirm(
        "Will build indices:\n{}\nContinue?".format(pprint.pformat(config.indices)),
        logger,
    )

    logger.info("Validating differences in mappings...")

    if "gene_expression" in config.indices and len(config.indices) == 1:
        return config

    non_null_fields = get_non_null_fields(config)

    if not non_null_fields:
        logger.info("No new breaking differences were found.")
        return config

    user_confirm(
        (
            "\nThe following fields have new values and are missing from the "
            "case_centric mappings:\n\n{}\n\nWould you like to extend the "
            "blacklist? NOTE: Skipping expand might result in ES index upload "
            "failure.\n".format("\n".join(non_null_fields))
        ),
        logger,
        on_confirm=partial(args.blacklist_fields.extend, non_null_fields),
        on_decline=no_op,
    )

    return config


def get_spark_args(args):
    """
    Returns list of spark related command line arguments and values for `spark-submit`
    """
    # Get list of eggs and jars to upload
    jars_dir = os.path.join(ROOT_DIR, "artifacts", "jars")
    eggs_dir = os.path.join(ROOT_DIR, "artifacts", "eggs")
    jars = [os.path.join(jars_dir, j) for j in os.listdir(jars_dir)]
    eggs = [os.path.join(eggs_dir, e) for e in os.listdir(eggs_dir)]
    app_egg = "gdc_mutation_indexer-{}_rev_{}-py3.5.egg".format(
        VERSION, get_git_commit(ROOT_DIR)
    )
    eggs.append(os.path.join(ROOT_DIR, "dist", app_egg))
    spark_args = ["--py-files", ",".join(eggs), "--jars", ",".join(jars)]

    # Add other spark arguments
    for key, value in SparkArgs().iter_args(args):
        name = "--{}".format(key)
        spark_args.extend([name, str(value)])

    for key, value in SparkConfArgs().iter_args(args):
        name = key.replace("-", ".")
        spark_args.extend(["--conf", "{}={}".format(name, value)])

    spark_args.extend(["--conf", "spark.sql.caseSensitive=True"])

    return spark_args


def get_config_args(args):
    """
    Returns list of configuration arguments and values for `spark-submit`
    """
    config_args = []
    for parser_cls in ALL_PARSERS:
        parser = parser_cls()
        for key, value in parser.iter_args(args):
            info = parser.arguments[key]

            varname = key.upper().replace("-", "_")
            if isinstance(value, list):
                value = ",".join(map(str, value))

            arg_action = info.get("action")
            # Do not pass bool flags if not needed
            # If default is True and value is True
            if arg_action == "store_false" and value == "True":
                continue
            # If default is False and value is False
            if arg_action == "store_true" and value == "False":
                continue
            config_args.extend(
                ["--conf", 'spark.yarn.appMasterEnv.{}="{}"'.format(varname, value)]
            )
            config_args.extend(
                ["--conf", 'spark.executorEnv.{}="{}"'.format(varname, value)]
            )

    return config_args


def get_submit_command(args):
    """
    Builds command to run to submit spark job
    """
    SPARK_HOME = os.getenv("SPARK_HOME")
    command = ["{}/bin/spark-submit".format(SPARK_HOME)]
    command.extend(get_spark_args(args))
    command.extend(get_config_args(args))
    command.append(os.path.join(ROOT_DIR, "bin/export.py"))
    return command


def get_created_indices(es, indices):
    return [index for index in indices if es.indices.exists(index)]


if __name__ == "__main__":
    # Parse and confirm arguments
    args = parse_args()
    config = confirm_args(args)
    # Assemble and run the command
    command = get_submit_command(args)
    subprocess.call(command)
    es = config.es
    # mutation indexer might have failed after building a subset of the indices
    # but the ones that were built might still be good, so we want to force-merge
    # whatever we have, force merge will fail if non exist index name in the list
    indices = get_created_indices(es, config.indices.values())

    if not indices:
        logger.info("No indices were built. Nothing to force merge")
        exit(0)

    esutils.force_merge_elasticsearch_indices(es, indices)
