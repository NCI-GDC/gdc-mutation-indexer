import argparse
import asyncio
import datetime
import itertools
import os
import pathlib
import tempfile
from os import path
from typing import Iterable, Optional, Tuple

import elasticsearch
import halo
import importlib_resources as resources
import more_itertools
import toml

from exports import configuration


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", type=pathlib.Path, default=None)

    return parser


def get_config(config_path: Optional[pathlib.Path]) -> configuration.Configuration:
    with resources.path(
        "exports.configuration", "default-configuration.toml"
    ) as default_file:
        default_config = toml.load(default_file)

    if config_path:
        user_config = toml.load(config_path)

        default_config.update(user_config)

    return configuration.CONGIF_SCHEMA.load(default_config)


def get_file_args(config: configuration.Configuration) -> Iterable[Tuple[str, str]]:
    build = config.build
    config_file = path.join(
        build.config_dir, f"mutation-index-config-{datetime.datetime.now()}.toml"
    )

    with open(config_file, "w+") as f:
        toml.dump(configuration.CONGIF_SCHEMA.dump(config), f)

    yield ("--files", config_file)
    yield (
        "--py-files",
        ",".join(path.join(egg, build.py_dir) for egg in os.listdir(build.py_dir)),
    )
    yield (
        "--jars",
        ",".join(path.join(jar, build.jar_dir) for jar in os.listdir(build.jar_dir)),
    )


async def run_spark_command(config: configuration.Configuration) -> None:
    arguments = config.spark_arguments.get_arguments()
    config_arguments = config.spark.get_arguments()
    file_arguments = get_file_args(config)
    arguments = more_itertools.flatten(
        itertools.chain(arguments, config_arguments, file_arguments)
    )
    spark_home = os.getenv("SPARK_HOME", "")
    spark_command = path.join(spark_home, "bin/spark-submit")
    final_command = " ".join(more_itertools.value_chain(spark_command, arguments))
    home_dir = os.environ.get("HOME", "")
    output_file = path.join(tempfile.tempdir or home_dir, "mutation-indexer.log")
    error_file = path.join(tempfile.tempdir or home_dir, "mutation-indexer-error.log")

    with open(output_file, "wb+") as out_f, open(error_file, "wb+") as error_f:
        process = await asyncio.create_subprocess_shell(
            final_command, stdout=out_f, stderr=error_f
        )

        await process.wait()

async def force_merge_indices(config: configuration.Configuration) -> None:
    es_client = elasticsearch.AsyncElasticsearch(
        config.elasticsearch.nodes.split(","),
        use_ssl=config.elasticsearch.use_ssl,
        verify_certs=config.elasticsearch.verify_certs,
        http_auth=(config.elasticsearch.user, config.elasticsearch.password),
    )
    loop = asyncio.get_event_loop()

    async def get_index(index: str) -> Optional[str]:
        if await es_client.indices.exists(index):
            return index

        return None

    tasks = asyncio.as_completed((loop.create_task(get_index(index)) for index in config.build.indices.value()))
    indices = filter(None, (task.result for task in tasks))

    for index in indices:
        await es_client.indices.forcemerge(index=index, max_num_segments=1)


async def main() -> None:
    parser = get_argument_parser()
    args = parser.parse_args()
    config = get_config(args.config)

    with halo.Halo(spinner="pong"):
        await run_spark_command(config)
        await force_merge_indices(config)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.run_until_complete(main())
