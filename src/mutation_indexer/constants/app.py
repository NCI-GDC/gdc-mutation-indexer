import enum

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s"
ROOT_MODULE = "mutation_indexer"
"""The root module for the application."""
CONFIGURATION_FILE = "configuration.toml"
"""A configuration file within the application."""
LOGGING_TOML = "logging.toml"
"""A configuration file for log formatting."""


class Driver(enum.StrEnum):
    """The driver modules in Mutation Indexer."""

    GENE_EXPRESSION = "gene-expression"
    VIZ = "viz"

    @property
    def module(self) -> str:
        return f"{ROOT_MODULE}.{self.value.replace('-', '_')}"
