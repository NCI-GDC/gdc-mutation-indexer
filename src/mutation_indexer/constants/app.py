CONFIGURATION_FILE = "configuration.toml"
"""The name of the configuration files within mutation indexer package."""
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s"
"""The format with which to configure the logger.

OBSOLETE: This has been handled by the logging module but a few modules still reference
this.
"""
ROOT_MODULE = "mutation_indexer"
"""The name of the root module for this application."""
