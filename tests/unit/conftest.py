import dataclasses
import os
from collections.abc import Generator, Iterable
from typing import Any

import pyspark
import pytest
import yaml
from pyspark import sql
from pyspark.sql import types

from tests.unit import utils


def pytest_addoption(parser: pytest.Parser) -> None:
    """This adds an option so we can run the tests & update any final schemas."""
    parser.addoption("--update-schemas", action="store_true")


@pytest.fixture(scope="session")
def spark_session() -> Generator[sql.SparkSession]:
    with (
        sql.SparkSession.builder.master("local[*]")
        .appName("unit-testing")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.ui.showConsoleProgress", False)
        .config("spark.ui.enabled", False)
        .config("spark.driver.memory", "2g")
        .getOrCreate() as spark_session
    ):
        spark_session.sparkContext.setLogLevel("FATAL")
        spark_session.sql("set spark.sql.caseSensitive=true")

        yield spark_session


@pytest.fixture(scope="session")
def create_dataframe(spark_session: sql.SparkSession) -> utils.CreateDataFrame:
    def inner(data: Iterable[Any], schema: types.StructType) -> sql.DataFrame:
        rdd: pyspark.RDD = spark_session.sparkContext.parallelize(
            map(dataclasses.asdict, data)
        )

        return spark_session.createDataFrame(rdd, schema)

    return inner


@pytest.fixture(scope="session")
def data_dir():
    current_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(current_path, "data")


@pytest.fixture
def fake_hits_and_expectations(data_dir):
    def load_hits_from_file(filename):
        with open(os.path.join(data_dir, filename)) as f:
            contents = yaml.safe_load(f)

        hits = contents["hits"]
        expected = contents["expected"]

        return (
            {"hits": [{"_id": hit.pop("_id"), "_source": hit} for hit in hits]},
            expected,
        )

    return load_hits_from_file


@pytest.fixture
def assert_schemas_equal(request: pytest.FixtureRequest) -> utils.AssertSchemasEqual:
    """Use this fixture when checking the final output schema of a build.

    NOTE: This fixture ensures when we run the tests with the `--update-schemas` flag that the
    final schemas of each builder are updated with their actual outputs. This is important
    when one or more of a builder's inputs change which happens for various reasons as the
    codebase changes.
    """
    are_schemas_updated = request.config.getoption("--update-schemas")

    return utils.AssertSchemasEqual(are_schemas_updated)
