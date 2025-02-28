import argparse
import asyncio
import contextlib
import datetime
import itertools
import logging
import os
import pathlib
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from importlib import resources
from typing import Any, cast

import elasticsearch
import halo
import more_itertools
import toml

import mutation_indexer
from mutation_indexer import configuration
from mutation_indexer.configuration import build, environment

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
    default_config = toml.loads(
        resources.read_text(mutation_indexer, "configuration.toml")
    )
    default_config["build"]["config_file"] = final_config_file
    user_config = toml.load(user_config_file)

    merge_dict(default_config, user_config)

    return default_config


def write_manifest(config: configuration.Configuration) -> None:
    """
    Writes the configuration data into the manifest with all secret values obfuscated.

    Args:
        config: the configuration with which the build was run.
    """
    build = config.build
    file_name = (
        build.manifest_dir
        / f"{datetime.datetime.now().isoformat()}-{build.build_id}.toml"
    )
    data: dict = configuration.OBFUSCATED_CONFIG_SCHEMA.dump(config)  # type: ignore

    build.manifest_dir.mkdir(parents=True, exist_ok=True)

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
    config = None

    try:
        with tempfile.TemporaryDirectory() as temp_directory:
            config_file = pathlib.Path(temp_directory) / "configuration.toml"
            config_data = load_config_data(user_config_file, config_file.as_posix())
            config = cast(
                configuration.Configuration,
                configuration.CONFIG_SCHEMA.load(config_data),
            )

            with open(config_file, "w+") as f:
                toml.dump(config_data, f)

            yield config

    finally:
        if config is not None:
            write_manifest(config)


def get_file_args(config: build.Build) -> Iterable[tuple[str, str]]:
    """
    Sets the spark-submit config values as well as jars params.

    Yields:
        a tuple of argument flag and value.
    """
    files = ",".join(
        (
            f"{config.config_file}#configuration.toml",
            f"{config.pex_file}#mutation-indexer.pex",
        )
    )

    yield (
        "--conf",
        f"spark.yarn.dist.files={files}",
    )
    yield (
        "--jars",
        ",".join(map(str, config.jar_dir.glob("**/*.jar"))),
    )


async def run_spark_command(config: configuration.Configuration) -> None:
    """
    Runs the spark-submit command which will spwan the spark application. The spark
    application will build the desired indices.

    Args:
        config: the configuration for the build.
    """
    config_arguments = config.spark.get_arguments()
    file_arguments = get_file_args(config.build)
    arguments = more_itertools.flatten(
        itertools.chain(config_arguments, file_arguments)
    )
    final_command = " ".join(
        more_itertools.value_chain(
            str(config.build.spark_submit),
            arguments,
            str(config.build.driver),
        )
    )

    with open(config.build.output_log, "wb+") as out_file, open(
        config.build.error_log, "wb+"
    ) as error_file:
        process = await asyncio.create_subprocess_shell(
            final_command, stdout=out_file, stderr=error_file
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
        indices = [
            config.elasticsearch.write.indices[i] for i in config.build.index_types
        ]

        try:
            await es_client.indices.forcemerge(
                index=indices, max_num_segments=1, ignore_unavailable=True
            )
        except Exception as ex:
            logger.warning(f"Error occurred while merging: {ex}.")


def set_environment_variables(env: environment.Environment) -> None:
    """
    Sets the environmental variables needed to run spark submit.

    Args:
        env: the configured values for the environmental variables.
    """
    os.environ["JAVA_HOME"] = env.java_home
    os.environ["SPARK_HOME"] = env.spark_home
    os.environ["YARN_CONF_DIR"] = env.yarn_conf_dir


async def _main() -> None:
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


def main() -> None:
    try:
        asyncio.run(_main())
    except:
        logger.critical("Application failed.", exc_info=True)


if __name__ == "__main__":
    main()
