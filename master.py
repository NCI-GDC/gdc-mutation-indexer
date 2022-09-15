import argparse
import asyncio
import contextlib
import datetime
import itertools
import logging
import os
import tempfile
import uuid
from os import path
from typing import Any, Iterable, Iterator, Mapping, Tuple

import elasticsearch
import halo
import importlib_resources as resources
import more_itertools
import toml

import exports
from exports import configuration
from exports.configuration import environment

ROOT_DIR = path.dirname(__file__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_argument_parser() -> argparse.ArgumentParser:
    """
    Get the argument parser for the client application.

    Returns:
        An argument parser with the required parameters to run the client application.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", type=str, default=None)

    return parser


def merge_dict(a: dict, b: dict) -> None:
    """
    Merges the dictionary values from b into a. Any dictionary values in b will be
    recursively merged into the value for a.

    Args:
        a: The target dictionary to be updated
        b: The dictionary containing the values to be merged into a
    """
    for key, value in b.items():
        if isinstance(value, dict):
            merge_dict(a.setdefault(key, {}), value)

        else:
            a[key] = value


def load_config_data(
    user_config_file: str, final_config_file: str
) -> Mapping[str, Any]:
    """
    Loads the user provided configuration and updates it with any required default
    values.

    Args:
        user_config_file: the configuration file provided by the user
        final_config_file: the file into which the final configuration data will be
            persisted into.

    Returns:
        the final configuration data as a mapping.
    """
    default_config = toml.loads(resources.read_text(exports, "configuration.toml"))
    default_config["build"]["config_file"] = final_config_file
    user_config = toml.load(user_config_file)

    merge_dict(default_config, user_config)

    return default_config


def get_manifest_file(manifest_dir: str, build_id: uuid.UUID) -> str:
    """
    Returns:
        the path to the manifest file into which the build's configuration will be
        recorded.
    """
    return path.join(
        manifest_dir,
        f"{datetime.datetime.now().isoformat()}-{build_id}.toml",
    )


def write_manifest(config: configuration.Configuration) -> None:
    """
    Writes the configuration data into the manifest with all secret values obfuscated.

    Args:
        config: the configuration with which the build was run.
    """
    build = config.build
    file_name = get_manifest_file(build.manifest_dir, build.build_id)
    data = configuration.OBFUSCATED_CONFIG_SCHEMA.dump(config)

    os.makedirs(build.manifest_dir, exist_ok=True)

    with open(file_name, "w+") as f:
        toml.dump(data, f)


@contextlib.contextmanager
def get_config(
    user_config_file: str,
) -> Iterator[configuration.Configuration]:
    """
    Loads the configuration data and persists the raw data into a temporary file which
    can be uploaded with the spark-submit command. The context manager returned insures
    that the temporary file is removed and that the data is obfuscated and stored in the
    manifest file.

    Args:
        user_config_file: The path to the user provided configuration file.

    Returns:
        A context manager which in turn provides the configuration object with which to
        run the application.
    """
    with tempfile.TemporaryDirectory() as temp_directory:
        config_file = path.join(temp_directory, "configuration.toml")
        config_data = load_config_data(user_config_file, config_file)
        config = configuration.CONFIG_SCHEMA.load(config_data)

        with open(config_file, "w+") as f:
            toml.dump(config_data, f)

        yield config

    write_manifest(config)


def get_file_args(config: configuration.Configuration) -> Iterable[Tuple[str, str]]:
    """
    Sets the spark-submit config values as well as jars params.

    Yields:
        a tuple of argument flag and value.
    """
    build = config.build
    files = ",".join(
        (
            f"{config.build.config_file}#configuration.toml",
            path.join(ROOT_DIR, "mutation-indexer.pex#mutation-indexer.pex"),
        )
    )

    yield (
        "--conf",
        f"spark.yarn.dist.files={files}",
    )
    yield (
        "--jars",
        ",".join(path.join(build.jar_dir, jar) for jar in os.listdir(build.jar_dir)),
    )


async def run_spark_command(config: configuration.Configuration) -> None:
    """
    Runs the spark-submit command which will spwan the spark application. The spark
    application will build the desired indices.

    Args:
        config: the configuration for the build.
    """
    config_arguments = config.spark.get_arguments()
    file_arguments = get_file_args(config)
    arguments = more_itertools.flatten(
        itertools.chain(config_arguments, file_arguments)
    )
    spark_home = os.getenv("SPARK_HOME", "")
    spark_command = path.join(spark_home, "bin/spark-submit")
    final_command = " ".join(
        more_itertools.value_chain(
            spark_command, arguments, path.join(ROOT_DIR, "bin/export.py")
        )
    )
    home_dir = os.environ.get("HOME", "")

    output_file = path.join(tempfile.gettempdir() or home_dir, "mutation-indexer.log")
    error_file = path.join(
        tempfile.gettempdir() or home_dir, "mutation-indexer-error.log"
    )

    with open(output_file, "wb+") as out_f, open(error_file, "wb+") as error_f:
        process = await asyncio.create_subprocess_shell(
            final_command, stdout=out_f, stderr=error_f
        )

        await process.wait()


async def force_merge_indices(config: configuration.Configuration) -> None:
    """
    Performs a force merge on the indices that have been created.

    Args:
        config: The configuration with which the build was run.
    """
    async with elasticsearch.AsyncElasticsearch(
        config.elasticsearch.connection.nodes.split(","),
        use_ssl=config.elasticsearch.connection.use_ssl,
        verify_certs=config.elasticsearch.connection.verify_certs,
        http_auth=(
            config.elasticsearch.connection.user,
            config.elasticsearch.connection.password,
        ),
    ) as es_client:
        to_merge = []

        for index in config.elasticsearch.write.indices.values():
            if await es_client.indices.exists(index=index):
                to_merge.append(index)
            else:
                logger.warning(f"Build failed to build index: {index}.")

        await es_client.indices.forcemerge(index=",".join(to_merge), max_num_segments=1)


def set_environment_variables(env: environment.Environment) -> None:
    """
    Sets the environmental variables needed to run spark submit.

    Args:
        env: the configured values for the environmental variables.
    """
    os.environ["JAVA_HOME"] = env.java_home
    os.environ["SPARK_HOME"] = env.spark_home
    os.environ["YARN_CONF_DIR"] = env.yarn_conf_dir


async def main() -> None:
    parser = get_argument_parser()
    args = parser.parse_args()

    with get_config(args.config) as config:
        print(f"RUNNING BUILD: {config.build.build_id}")
        set_environment_variables(config.environment)

        with halo.Halo(spinner="pong") as spinner:
            try:
                spinner.text = "Running spark-submit"
                await run_spark_command(config)
                spinner.text = "Merging indices"
                await force_merge_indices(config)
            except:
                spinner.fail("Process Failed")
                raise
            else:
                spinner.succeed("Indices built")


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        logger.critical("Appliction failed.", exc_info=True)
