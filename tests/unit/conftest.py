import os
from typing import Callable, Generator, Iterable

import pytest
import yaml
from pyspark import sql
from pyspark.sql import types


@pytest.fixture(scope="session")
def spark_session() -> Generator[sql.SparkSession, None, None]:
    with sql.SparkSession.builder.master("local[*]").appName(
        "sqlContextFixture"
    ).config("spark.sql.shuffle.partitions", 1).config(
        "spark.ui.showConsoleProgress", False
    ).config(
        "spark.ui.enabled", False
    ).config(
        "spark.driver.memory", "2g"
    ).getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")
        spark_session.sql("set spark.sql.caseSensitive=true")

        yield spark_session


@pytest.fixture(scope="session")
def create_dataframe(
    spark_session: sql.SparkSession,
) -> Callable[[Iterable, types.StructType], sql.DataFrame]:
    def _create_dataframe(data: Iterable, schema: types.StructType) -> sql.DataFrame:
        return spark_session.sparkContext.parallelize(data).toDF(schema)

    return _create_dataframe


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
