import argparse
import asyncio
import contextlib
import datetime
import itertools
import os
import pathlib
import tempfile
from os import path
from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple

import elasticsearch
import halo
import importlib_resources as resources
import more_itertools
import toml

import exports
from exports import configuration
from exports.configuration import environment

ROOT_DIR = path.dirname(__file__)


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", type=str, default=None)

    return parser


def merge_dict(a: dict, b: dict) -> None:
    for key, value in b.items():
        if isinstance(value, dict):
            merge_dict(a.setdefault(key, {}), value)

        else:
            a[key] = value


def load_config_data(config_path: str, temp_config_file: str) -> Mapping[str, Any]:
    default_config = toml.loads(resources.read_text(exports, "configuration.toml"))
    default_config["build"]["config_file"] = temp_config_file

    if config_path:
        user_config = toml.load(config_path)

        merge_dict(default_config, user_config)

    return default_config


def write_manifest(config: configuration.Configuration) -> None:
    file_name = path.join(
        config.build.config_dir,
        f"{config.build.build_id}-{datetime.datetime.now().isoformat()}.toml",
    )
    data = configuration.OBFUSCATED_CONFIG_SCHEMA.dump(config)

    os.makedirs(config.build.config_dir, exist_ok=True)

    with open(file_name, "w+") as f:
        toml.dump(data, f)


@contextlib.contextmanager
def get_config(
    config_path: Optional[pathlib.Path],
) -> Iterator[configuration.Configuration]:
    with tempfile.NamedTemporaryFile("w+", suffix=".toml") as f:
        config_data = load_config_data(config_path, f.name)
        config = configuration.CONFIG_SCHEMA.load(config_data)

        toml.dump(config_data, f)

        yield config

    write_manifest(config)


def get_file_args(config: configuration.Configuration) -> Iterable[Tuple[str, str]]:
    build = config.build

    yield (
        "--files",
        ",".join(
            (
                f"{config.build.config_file}#configuration.toml",
                path.join(ROOT_DIR, "mutation-indexer.pex#mutation-indexer.pex"),
            )
        ),
    )
    yield (
        "--jars",
        ",".join(path.join(build.jar_dir, jar) for jar in os.listdir(build.jar_dir)),
    )


async def run_spark_command(config: configuration.Configuration) -> None:
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
    async with elasticsearch.AsyncElasticsearch(
        config.elasticsearch.connection.nodes.split(","),
        use_ssl=config.elasticsearch.connection.use_ssl,
        verify_certs=config.elasticsearch.connection.verify_certs,
        http_auth=(
            config.elasticsearch.connection.user,
            config.elasticsearch.connection.password,
        ),
    ) as es_client:
        indices = (
            index
            for index in config.build.indices.values()
            if await es_client.indices.exists(index=index)
        )

        async for index in indices:
            await es_client.indices.forcemerge(index=index, max_num_segments=1)


def set_environment_variables(env: environment.Environment) -> None:
    os.environ["JAVA_HOME"] = env.java_home
    os.environ["SPARK_HOME"] = env.spark_home
    os.environ["YARN_CONF_DIR"] = env.yarn_conf_dir


async def main() -> None:
    parser = get_argument_parser()
    args = parser.parse_args()

    with get_config(args.config) as config:
        print(f"RUNNING BUILD: {config.build.build_id}")
        set_environment_variables(config.environment)

        with halo.Halo(spinner="pong"):
            await run_spark_command(config)
            await force_merge_indices(config)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.run_until_complete(main())
