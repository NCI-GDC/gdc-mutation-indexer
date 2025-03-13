import argparse
import asyncio
import itertools
import logging
import pathlib
from collections.abc import Iterable, Sequence

import elasticsearch
import halo
import more_itertools

from mutation_indexer import configuration
from mutation_indexer.configuration import build

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_argument_parser() -> argparse.ArgumentParser:
    """
    Get the argument parser for the client application.

    Returns:
        An argument parser with the required parameters to run the client application.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("configs", type=pathlib.Path, nargs="*")

    return parser


def get_file_args(config: build.Build) -> Iterable[tuple[str, str]]:
    """
    Sets the spark-submit config values as well as jars params.

    Yields:
        a tuple of argument flag and value.
    """
    files = ",".join(
        (
            f"{config.config_file}#configuration.toml",
            f"{config.pex_file}#python",
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

    with (
        open(config.build.output_log, "wb+") as out_file,
        open(config.build.error_log, "wb+") as error_file,
    ):
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


class Args(argparse.Namespace):
    configs: Sequence[pathlib.Path]


async def _main() -> None:
    parser = get_argument_parser()
    args = parser.parse_args(namespace=Args())

    with configuration.Configuration.initialize(args.configs) as config:
        print(f"RUNNING BUILD: {config.build.build_id}")

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
