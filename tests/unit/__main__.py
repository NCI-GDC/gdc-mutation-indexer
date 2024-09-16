import unittest
from typing import Optional

import more_itertools
import testtools

from tests.unit import fixtures


class HashableTestSuite(unittest.TestSuite):
    def __hash__(self) -> int:
        return id(self)


def make_tests(suite: unittest.TestSuite):
    tests = testtools.iterate_tests(suite)
    tests_by_class = more_itertools.map_reduce(tests, keyfunc=lambda t: type(t))

    return (HashableTestSuite(ts) for ts in tests_by_class.values())


class TestLoader(unittest.TestLoader):
    def discover(
        self,
        start_dir: str,
        pattern: str = "test*.py",
        top_level_dir: Optional[str] = None,
    ) -> unittest.TestSuite:
        suite = super().discover(start_dir, pattern, top_level_dir)

        return testtools.ConcurrentTestSuite(suite, make_tests=make_tests)


if __name__ == "__main__":
    with fixtures.setup_fixtures():
        unittest.main(module=None, testLoader=TestLoader())
