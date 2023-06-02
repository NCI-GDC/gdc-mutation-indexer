import io

import click

from tests.unit.data.schemas import _minimize, _translate_mapping


@click.group
def cli() -> None:
    pass


@cli.command()
@click.option(
    "--schemas",
    "-s",
    default="tests.unit.data.schemas",
    type=str,
    help="A single schema yaml file or a module or directory containing yaml schema files.",
)
def minimize(schemas: str) -> None:
    """A tool for minimizing yaml files using anchors to reduce the amount of repeated data."""
    _minimize.minimize_files(schemas)


@cli.command()
@click.option(
    "--mapping", "-m", type=click.File(mode="r"), help="The path to the mappings file."
)
@click.option(
    "--output",
    "-o",
    type=click.File(mode="w+"),
    help="The location which the output schema will be written.",
)
@click.option(
    "--include",
    "-i",
    default="",
    type=str,
    help="A comma separated list of properties in the root mapping to include.",
)
def translate_mapping(
    mapping: io.TextIOBase, output: io.TextIOBase, include: str
) -> None:
    """
    A tool for translating an elasticsearch mapping into a spark schema. WARNING: this
    process cannot recognize arrays of atomic values as ES mappings does not distinguish
    these from regular atomic values.
    """
    _translate_mapping.translate(mapping, output, include)


if __name__ == "__main__":
    cli()
