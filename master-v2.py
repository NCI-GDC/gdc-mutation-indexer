import argparse
import asyncio
import pathlib
import tempfile
from typing import Optional

import importlib_resources as resources
import marshmallow_dataclass
import more_itertools
import toml
import yaml

from exports import configuration
import elasticsearch


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", type=pathlib.Path, default=None)

    return parser


def get_config(config_path: Optional[pathlib.Path]) -> configuration.Configuration:
    schema = marshmallow_dataclass.class_schema(configuration.Configuration)()

    with resources.path(
        "exports.configuration", "default-configuration.toml"
    ) as default_file:
        default_config = toml.load(default_file)

    if config_path:
        user_config = toml.load(config_path)

        default_config.update(user_config)

    return schema.load(default_config)


async def run_spark_command(config: configuration.Configuration) -> None:
    schema = marshmallow_dataclass.class_schema(configuration.Configuration)()
    arguments = config.spark_arguments.get_arguments()
    config_arguments = config.spark.get_arguments()

    with tempfile.TemporaryFile("w") as config_file:
        toml.dump(schema.dump(config), config_file)

        files_arg = ("--files", config_file.name)

        print(list(more_itertools.value_chain(arguments, config_arguments, files_arg)))


async def force_merge_indices(
    config: configuration.Configuration, es_client: elasticsearch.AsyncElasticsearch
) -> None:
    indices = (
        index
        for index in config.build.indices.values()
        if es_client.indices.exists(index)
    )

    task = await es_client.indices.forcemerge(
        indices, max_num_segments=1, wait_for_completion=False
    )


async def main() -> None:
    parser = get_argument_parser()
    args = parser.parse_args()
    config = get_config(args.config)
    es_client = elasticsearch.AsyncElasticsearch(
        config.elasticsearch.nodes.split(","),
        use_ssl=config.elasticsearch.use_ssl,
        verify_certs=config.elasticsearch.verify_certs,
        http_auth=(config.elasticsearch.user, config.elasticsearch.password),
    )

    await run_spark_command(config)

    # force_merge_indices(config, es_client)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.run_until_complete(main())
