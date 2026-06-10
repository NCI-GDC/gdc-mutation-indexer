import pytest


def pytest_addoption(parser: pytest.Parser, pluginmanager: pytest.PytestPluginManager) -> None:
    """This adds an option so we can run the tests & update any final schemas."""
    parser.addoption("--update-schemas", action="store_true")
