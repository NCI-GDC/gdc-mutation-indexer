import asyncio
import logging
import pathlib

import click

from mutation_indexer import client, gene_expression, viz

logger = logging.getLogger()


@click.group()
def cli():
    pass


def _start(client: client.Client, config: pathlib.Path) -> None:
    try:
        asyncio.run(client.run(config))
    except Exception as ex:
        logger.critical("Client Failed.", exc_info=ex)


@cli.command("viz")
@click.option(
    "--config",
    "-c",
    type=click.Path(
        exists=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path
    ),
)
def start_viz(config: pathlib.Path):
    client = viz.Client()

    _start(client, config)


@cli.command("gene_expression")
@click.option(
    "--config",
    "-c",
    type=click.Path(
        exists=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path
    ),
)
def start_gene_expression(config: pathlib.Path):
    client = gene_expression.Client()

    _start(client, config)
