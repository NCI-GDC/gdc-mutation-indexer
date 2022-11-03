import os
from typing import Generator

from pyspark import sql
import pytest

_spark_session = None


def pytest_sessionstart(session: pytest.Session) -> None:
    global _spark_session

    _spark_session = (
        sql.SparkSession.builder.master("local[*]")
        .appName("sqlContextFixture")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.ui.showConsoleProgress", False)
        .config("spark.ui.enabled", False)
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    _spark_session.sparkContext.setLogLevel("FATAL")
    _spark_session.sql("set spark.sql.caseSensitive=true")


@pytest.fixture(scope="session")
def spark_session() -> sql.SparkSession:
    global _spark_session

    assert _spark_session, "Spark session not initialized"

    return _spark_session


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _spark_session

    if _spark_session:
        _spark_session.stop()


@pytest.fixture(scope="session")
def data_dir():
    current_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(current_path, "data")
