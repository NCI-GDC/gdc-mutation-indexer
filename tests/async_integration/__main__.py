import asyncio
from collections.abc import Iterator
import contextlib
import unittest

from pyspark import sql

from mutation_indexer import configuration
from tests.async_integration import setup

config: configuration.Configuration
spark_session: sql.SparkSession


class TestSuite(unittest.TestSuite):
    def __init__(
        self, tests: configuration.Iterable[unittest.TestCase | unittest.TestSuite]
    ) -> None:
        super().__init__(tests)

        self._loop = asyncio.get_event_loop()
        self._context = contextlib.AsyncExitStack()

    @contextlib.contextmanager
    def _setup(self) -> Iterator:
        global config
        global spark_session

        try:
            config = self._context.enter_context(setup.ConfigurationSetup())
            _ = self._loop.run_until_complete(
                self._context.enter_async_context(
                    setup.ESIndicesSetup(config.elasticsearch)
                )
            )
            spark_session = self._context.enter_context(setup.SparkSessionSetup())
            _ = self._loop.run_until_complete(
                self._context.enter_async_context(setup.MutationIndexerSetup(config))
            )

            yield None
        finally:
            self._loop.run_until_complete(self._context.aclose())

    def run(
        self, result: unittest.TestResult, debug: bool = False
    ) -> unittest.TestResult:
        with self._setup():
            return super().run(result, debug)


class TestLoader(unittest.TestLoader):
    suiteClass = TestSuite


if __name__ == "__main__":
    unittest.main(testLoader=TestLoader())
