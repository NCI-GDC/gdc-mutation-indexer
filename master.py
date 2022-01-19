import argparse
import functools
import logging
import os
import pprint
import subprocess
from typing import Callable, Iterable, List, Sequence

import elasticsearch
import psqlgraph
from gdcdatamodel import models
from gdcmodels import esutils

import config
import parsers
from exports import es_utils

logging.basicConfig(format=config.LOG_FORMAT)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def get_release_info():
    """
    Lookup release candidate name and version in postgres
    """
    postgres_driver = psqlgraph.PsqlGraphDriver(
        os.environ["PG_HOST"],
        os.environ["PG_USER"],
        os.environ["PG_PASS"],
        os.environ["PG_NAME"],
    )
    with postgres_driver.session_scope():
        release_node = (
            postgres_driver.nodes(models.DataRelease).props(released=False).first()
        )
    release_name = release_node.name
    version = [release_node.major_version, release_node.minor_version]
    return release_name, version


def parse_args() -> argparse.Namespace:
    """
    Parse mutation indexer arguments
    """
    parser = parsers.ParserBuilder.build(
        config.ALL_PARSERS, description="Mutation Indexer"
    )
    args = parser.parse_args()
    return args


def no_op() -> None:
    """
    Callable that does nothing
    """
    pass


def raise_on_decline() -> None:
    """
    Raise a generic exception
    """
    raise Exception("User refused to continue")


def user_confirm(
    prompt: str,
    log: logging.Logger,
    on_confirm: Callable[[], None] = no_op,
    on_decline: Callable[[], None] = raise_on_decline,
) -> None:
    """
    Prompt user confirmation to proceed

    :param prompt_string: prompt string
    :param log: logging instance
    :param on_confirm: callable to invoke when user confirms
    :param on_decline: callable to invoke when user declines
    """
    while True:
        log.info(prompt)
        ans = input().lower()
        if ans in ["y", "yes"]:
            return on_confirm()
        elif ans in ["n", "no"]:
            return on_decline()
        else:
            log.error(f"Invalid answer: {ans}")


def confirm_args(args: argparse.Namespace) -> config.BaseConfig:
    """
    Confirm with user that args and index names are as expected
    """
    # Log arguments
    parsers.ParserBuilder.log_args(args, config.ALL_PARSERS, logger)

    # Initialize config with environment variables (the way spark worker will see it)
    env_dict = parsers.ParserBuilder.get_environment_dict(args, config.ALL_PARSERS)
    configuration = config.BaseConfig(env_dict=env_dict)

    # Confirm with user
    user_confirm(
        f"Will build indices:\n{pprint.pformat(configuration.indices)}\nContinue?",
        logger,
    )

    logger.info("Validating differences in mappings...")

    if "gene_expression" in configuration.indices and len(configuration.indices) == 1:
        return configuration

    non_null_fields = es_utils.get_non_null_fields(configuration)

    if not non_null_fields:
        logger.info("No new breaking differences were found.")
        return configuration

    user_confirm(
        (
            "\nThe following fields have new values and are missing from the "
            "case_centric mappings:\n\n{}\n\nWould you like to extend the "
            "blacklist? NOTE: Skipping expand might result in ES index upload "
            "failure.\n".format("\n".join(non_null_fields))
        ),
        logger,
        on_confirm=functools.partial(args.blacklist_fields.extend, non_null_fields),
        on_decline=no_op,
    )

    return configuration


def get_spark_args(args: argparse.Namespace) -> Iterable[str]:
    """
    Returns list of spark related command line arguments and values for `spark-submit`
    """
    # Get list of eggs and jars to upload
    jars_dir = os.path.join(config.ROOT_DIR, "artifacts", "jars")
    eggs_dir = os.path.join(config.ROOT_DIR, "artifacts", "eggs")
    jars = [os.path.join(jars_dir, j) for j in os.listdir(jars_dir)]
    eggs = [os.path.join(eggs_dir, e) for e in os.listdir(eggs_dir)]
    app_egg = f"gdc_mutation_indexer-{config.VERSION}-py{config.PYTHON_VERSION}.egg"
    eggs.append(os.path.join(config.ROOT_DIR, "dist", app_egg))
    spark_args = ["--py-files", ",".join(eggs), "--jars", ",".join(jars)]

    # Add other spark arguments
    for key, value in parsers.SparkArgs().iter_args(args):
        name = f"--{key}"
        spark_args.extend((name, str(value)))

    for key, value in parsers.SparkConfArgs().iter_args(args):
        name = key.replace("-", ".")
        spark_args.extend(("--conf", f"{name}={value}"))

    spark_args.extend(("--conf", "spark.sql.caseSensitive=True"))

    return spark_args


def get_config_args(args: argparse.Namespace) -> Iterable[str]:
    """
    Returns list of configuration arguments and values for `spark-submit`
    """
    config_args: List[str] = []
    for parser_cls in config.ALL_PARSERS:
        parser = parser_cls()  # type: ignore
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
                ("--conf", f'spark.yarn.appMasterEnv.{varname}="{value}"')
            )
            config_args.extend(("--conf", f'spark.executorEnv.{varname}="{value}"'))
    config_args.extend(
        ("--conf", f"spark.pyspark.python=python{config.PYTHON_VERSION}")
    )

    return config_args


def get_submit_command(args: argparse.Namespace) -> Sequence[str]:
    """
    Builds command to run to submit spark job
    """
    SPARK_HOME = os.getenv("SPARK_HOME")
    command = [f"{SPARK_HOME}/bin/spark-submit"]
    command.extend(get_spark_args(args))
    command.extend(get_config_args(args))
    command.append(os.path.join(config.ROOT_DIR, "bin/export.py"))
    return command


def get_created_indices(
    es: elasticsearch.Elasticsearch, indices: Iterable[str]
) -> List[str]:
    return [index for index in indices if es.indices.exists(index)]


if __name__ == "__main__":
    # Parse and confirm arguments
    args = parse_args()
    configuration = confirm_args(args)
    # Assemble and run the command
    command = get_submit_command(args)
    subprocess.call(command)
    es = configuration.es
    # mutation indexer might have failed after building a subset of the indices
    # but the ones that were built might still be good, so we want to force-merge
    # whatever we have, force merge will fail if non exist index name in the list
    indices = get_created_indices(es, configuration.indices.values())

    if not indices:
        logger.info("No indices were built. Nothing to force merge")
        exit(0)

    esutils.force_merge_elasticsearch_indices(es, indices)
