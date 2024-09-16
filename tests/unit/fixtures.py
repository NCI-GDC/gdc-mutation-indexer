import contextlib
from collections.abc import Iterator

import importlib_resources as resources
from pyspark import sql

__all__ = ("DATA_DIR", "SPARK_SESSION")


@contextlib.contextmanager
def setup_fixtures() -> Iterator[None]:
    with contextlib.ExitStack() as stack:
        global DATA_DIR
        DATA_DIR = stack.enter_context(
            resources.as_file(resources.files("tests.unit.data"))
        )

        global SPARK_SESSION
        SPARK_SESSION = stack.enter_context(
            sql.SparkSession.builder.master("local[*]")
            .appName("sqlContextFixture")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.ui.showConsoleProgress", False)
            .config("spark.ui.enabled", False)
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )

        SPARK_SESSION.sparkContext.setLogLevel("FATAL")
        SPARK_SESSION.sql("set spark.sql.caseSensitive=true")

        yield None
