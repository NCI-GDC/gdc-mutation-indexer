import os
import threading
from typing import Optional

import pytest
from pyspark import sql

_spark_session: Optional[sql.SparkSession] = None
_session_lock = threading.Lock()


@pytest.fixture(scope="session", autouse=True)
def spark_session() -> sql.SparkSession:
    global _spark_session

    with _session_lock:
        if not _spark_session:
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

        return _spark_session


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _spark_session

    if _spark_session:
        _spark_session.stop()


@pytest.fixture(scope="session")
def data_dir():
    current_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(current_path, "data")
