"""These tests act as sanity checks that the driver loads the configured builders and
calls them in a valid order.
"""

import contextlib
import inspect
from collections.abc import Iterator
from unittest import mock

from mutation_indexer.builders import bases
from mutation_indexer.builders import gene_expression as builders
from mutation_indexer.builders import gene_model
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import driver


@contextlib.contextmanager
def mock_builder(
    builder: type[bases.Builder], is_called: bool = True
) -> Iterator[mock.MagicMock]:
    """Mocks the given builder and ensures that it is properly called or not.

    Args:
        builder: The builder type which needs to be mocked out.
        is_called: A flag indicating that the builder.build method should be called
            during the run of the test.

    Returns:
        A context manager wrapping the mocked state of the given builder class.
    """
    params = inspect.signature(builder.build).parameters
    df_params = frozenset(p for p in params if p.endswith("_df"))

    with mock.patch.object(
        builder,
        "build",
        new=mock.MagicMock(__signature__=inspect.signature(builder.build)),
    ) as build_method:
        yield build_method

        if is_called:
            build_method.assert_called_once()
            assert df_params <= build_method.mock_calls[0].kwargs.keys()
        else:
            build_method.assert_not_called()


def test__driver__runs_all() -> None:
    with contextlib.ExitStack() as stack:
        config = mock.MagicMock()
        config.build.index_types = (build.IndexType.GENE_EXPRESSION,)

        stack.enter_context(mock_builder(gene_model.GeneModelBuilder))
        stack.enter_context(mock_builder(builders.PrimaryAliquotBuilder))
        stack.enter_context(mock_builder(builders.ExpressionValueBuilder))
        stack.enter_context(mock_builder(builders.IndexBuilder))
        # Mock out these factory functions used by the driver
        stack.enter_context(mock.patch("mutation_indexer.driver.get_es_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver.get_index_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver._initialize_spark"))

        driver.Driver().run(config)


def test__driver__runs_inputs() -> None:
    with contextlib.ExitStack() as stack:
        config = mock.MagicMock()
        config.build.index_types = ()

        stack.enter_context(mock_builder(gene_model.GeneModelBuilder))
        stack.enter_context(mock_builder(builders.PrimaryAliquotBuilder))
        stack.enter_context(mock_builder(builders.ExpressionValueBuilder))
        stack.enter_context(mock_builder(builders.IndexBuilder, is_called=False))
        # Mock out these factory functions used by the driver
        stack.enter_context(mock.patch("mutation_indexer.driver.get_es_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver.get_index_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver._initialize_spark"))

        driver.Driver().run(config)
