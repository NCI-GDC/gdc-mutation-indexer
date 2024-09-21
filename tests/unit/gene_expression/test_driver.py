import contextlib
from collections.abc import Iterable, Iterator
from unittest import mock

import pytest
from pyspark import sql

from mutation_indexer import builders
from mutation_indexer.gene_expression import driver


@pytest.fixture(scope="module")
def patch_build_method() -> Iterator[Iterable[mock.MagicMock]]:
    ge_builder = (
        builders.GeneModelBuilder,
        builders.GeneExpressionPrimaryAliquotBuilder,
        builders.GeneExpressionIndexBuilder,
    )

    with contextlib.ExitStack() as stack:
        yield tuple(
            stack.enter_context(
                mock.patch.object(b, "build", mock.create_autospec(b.build))
            )
            for b in ge_builder
        )


@pytest.fixture(scope="module")
def patch_spark_session() -> Iterator[mock.MagicMock]:
    with mock.patch.object(sql.SparkSession, "builder") as builder:
        yield builder


@pytest.mark.usefixtures("patch_build_method", "patch_spark_session")
def test__run__calls_all_builders(patch_build_method: Iterable[mock.MagicMock]) -> None:
    config = mock.MagicMock()

    driver.Driver.run(config)

    for method in patch_build_method:
        method.assert_called_once()
