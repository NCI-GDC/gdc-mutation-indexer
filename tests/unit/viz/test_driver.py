"""These tests act as sanity checks that the driver loads the configured builders and
calls them in a valid order.
"""

import contextlib
import inspect
from collections.abc import Iterator
from unittest import mock

from mutation_indexer import builders
from mutation_indexer.builders import base_builder, bases, civic
from mutation_indexer.constants import build
from mutation_indexer.viz import driver


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


@contextlib.contextmanager
def mock_base_builder(
    builder: type[base_builder.BaseBuilder], is_called: bool = True
) -> Iterator[tuple[mock.MagicMock, mock.MagicMock]]:
    """Mocks the given builder and ensures that it is properly called or not.

    Args:
        builder: The base builder type which needs to be mocked out.
        is_called: A flag indicating that the builder.build method should be called
            during the run of the test.

    Returns:
        A context manager wrapping the mocked state of the given builder class.
    """
    params = inspect.signature(builder.build).parameters
    df_params = frozenset(p for p in params if p.endswith("_df"))

    with (
        mock.patch.object(
            builder,
            "build",
            new=mock.MagicMock(
                __signature__=inspect.signature(builder.build), return_value=builder
            ),
        ) as build_method,
        mock.patch.object(builder, "load") as load_method,
    ):
        setattr(builder, builder.index_name, mock.MagicMock())

        yield build_method, load_method

        if is_called:
            build_method.assert_called_once()
            assert df_params <= build_method.mock_calls[0].kwargs.keys(), (
                build_method.mock_calls
            )
            load_method.assert_called_once_with()
        else:
            build_method.assert_not_called()
            load_method.assert_not_called()


def test__driver__runs_all() -> None:
    with contextlib.ExitStack() as stack:
        config = mock.MagicMock()
        config.build.index_types = (
            build.IndexType.CASE_CENTRIC,
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.GENE_CENTRIC,
            build.IndexType.SEGMENT_CNV_CENTRIC,
            build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        )

        stack.enter_context(mock_builder(builders.ASCATBuilder))
        stack.enter_context(mock_builder(builders.ASCATMetadataBuilder))
        stack.enter_context(mock_builder(builders.CaseBuilder))
        stack.enter_context(mock_base_builder(builders.CaseCentricBuilder))
        stack.enter_context(mock_builder(civic.DNABuilder))
        stack.enter_context(mock_builder(civic.ProteinBuilder))
        stack.enter_context(mock_base_builder(builders.CNVCentricBuilder))
        stack.enter_context(mock_base_builder(builders.CNVOccurrenceCentricBuilder))
        stack.enter_context(mock_base_builder(builders.GeneCentricBuilder))
        stack.enter_context(mock_builder(builders.GeneModelBuilder))
        stack.enter_context(mock_builder(builders.MAFBuilder))
        stack.enter_context(mock_builder(builders.MAFMetadataBuilder))
        stack.enter_context(mock_builder(builders.PrimaryAliquotBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVMetadataBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVCentricBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVOccurrenceCentricBuilder))
        stack.enter_context(mock_base_builder(builders.SSMCentricBuilder))
        stack.enter_context(mock_base_builder(builders.SSMOccurrenceCentricBuilder))
        # Mock out these factory functions used by the driver
        stack.enter_context(mock.patch("mutation_indexer.driver.get_es_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver.get_index_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver._initialize_spark"))

        driver.Driver().run(config)


def test__driver__runs_subset() -> None:
    with contextlib.ExitStack() as stack:
        config = mock.MagicMock()
        config.build.index_types = (
            build.IndexType.CASE_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
        )

        stack.enter_context(mock_builder(builders.ASCATBuilder))
        stack.enter_context(mock_builder(builders.ASCATMetadataBuilder))
        stack.enter_context(mock_builder(builders.CaseBuilder))
        stack.enter_context(mock_base_builder(builders.CaseCentricBuilder))
        stack.enter_context(mock_builder(civic.DNABuilder))
        stack.enter_context(mock_builder(civic.ProteinBuilder))
        stack.enter_context(mock_base_builder(builders.CNVCentricBuilder, is_called=False))
        stack.enter_context(mock_base_builder(builders.CNVOccurrenceCentricBuilder))
        stack.enter_context(mock_base_builder(builders.GeneCentricBuilder, is_called=False))
        stack.enter_context(mock_builder(builders.GeneModelBuilder))
        stack.enter_context(mock_builder(builders.MAFBuilder))
        stack.enter_context(mock_builder(builders.MAFMetadataBuilder))
        stack.enter_context(mock_builder(builders.PrimaryAliquotBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVMetadataBuilder))
        stack.enter_context(mock_builder(builders.SegmentCNVCentricBuilder, is_called=False))
        stack.enter_context(mock_builder(builders.SegmentCNVOccurrenceCentricBuilder))
        stack.enter_context(mock_base_builder(builders.SSMCentricBuilder))
        stack.enter_context(
            mock_base_builder(builders.SSMOccurrenceCentricBuilder, is_called=False)
        )
        # Mock out these factory functions used by the driver
        stack.enter_context(mock.patch("mutation_indexer.driver.get_es_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver.get_index_client"))
        stack.enter_context(mock.patch("mutation_indexer.driver._initialize_spark"))

        driver.Driver().run(config)
