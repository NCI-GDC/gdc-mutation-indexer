import click

from tests.integration.data import _vestigial_data


@click.group
def cli() -> None: ...


@cli.command("remove-vestigial")
def remove_vestigial() -> None:
    """Removes any vestigial properties & their values from the test data."""
    _vestigial_data.remove()


if __name__ == "__main__":
    cli()
