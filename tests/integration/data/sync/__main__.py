import click

from tests.integration.data.sync import _removal


@click.group
def cli() -> None: ...


@cli.command
def removal() -> None:
    """Removes any vestigial properties & their values from the test data."""
    _removal.remove_data()


if __name__ == "__main__":
    cli()
