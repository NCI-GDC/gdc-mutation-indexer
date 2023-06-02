import io

import click

from tests.unit.data.models import _create


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name", "-n", type=str, help="The name of the base model class.")
@click.option(
    "--input-schema",
    "-s",
    type=click.File(mode="rb"),
    help="The schema for which to create a model.",
)
@click.option(
    "--default-values",
    "-d",
    type=click.File(mode="rb"),
    help="A yaml file with the default property values.",
)
@click.option(
    "--output-file",
    "-o",
    type=click.File(mode="w+"),
    help="The file that the resulting models will be written to.",
)
@click.option(
    "--include-asserts",
    "-a",
    is_flag=True,
    default=False,
    help="This flag will include assert methods in the models generated.",
)
def create_model(
    name: str,
    input_schema: io.BytesIO,
    default_values: io.BytesIO,
    output_file: io.TextIOBase,
    include_asserts: bool,
) -> None:
    """
    A tool for creating the models for objects that represent rows in the input schema.
    """
    _create.create_model(
        name, input_schema, default_values, output_file, include_asserts
    )


if __name__ == "__main__":
    cli()
