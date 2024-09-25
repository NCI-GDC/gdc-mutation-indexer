import contextlib
import itertools
from collections.abc import Mapping
from typing import Iterator, Union
from unittest import mock

import pytest
from pyspark import sql

from mutation_indexer import builders
from mutation_indexer.builders import bases, civic
from mutation_indexer.constants import build
from mutation_indexer.viz import driver


@pytest.fixture
def patch_build_method() -> (
    Iterator[Mapping[type[Union[bases.Builder, builders.BaseBuilder]], mock.MagicMock]]
):
    index_builders = (
        builders.CaseCentricBuilder,
        builders.CNVCentricBuilder,
        builders.CNVOccurrenceCentricBuilder,
        builders.GeneCentricBuilder,
        builders.SSMCentricBuilder,
        builders.SSMOccurrenceCentricBuilder,
    )
    input_builders = (
        builders.ASCATMetadataBuilder,
        builders.ASCATBuilder,
        builders.CaseBuilder,
        civic.DNABuilder,
        civic.ProteinBuilder,
        builders.GeneModelBuilder,
        builders.MAFBuilder,
        builders.MAFMetadataBuilder,
        builders.PrimaryAliquotBuilder,
    )

    with contextlib.ExitStack() as stack:
        for builder in index_builders:
            stack.enter_context(
                mock.patch.object(builder, builder.index_name, create=True)
            )

        yield {
            b: stack.enter_context(
                mock.patch.object(b, "build", mock.create_autospec(b.build))
            )
            for b in itertools.chain(index_builders, input_builders)
        }


@pytest.fixture(scope="module")
def patch_spark_session() -> Iterator[mock.MagicMock]:
    with mock.patch.object(sql.SparkSession, "builder") as builder:
        yield builder


@pytest.mark.usefixtures("patch_spark_session")
def test__run__calls_all_builders(
    patch_build_method: Mapping[
        type[Union[bases.Builder, builders.BaseBuilder]], mock.MagicMock
    ]
) -> None:
    config = mock.MagicMock(
        build=mock.MagicMock(
            index_types=(
                build.IndexType.CASE_CENTRIC,
                build.IndexType.CNV_CENTRIC,
                build.IndexType.CNV_OCCURRENCE_CENTRIC,
                build.IndexType.GENE_CENTRIC,
                build.IndexType.SSM_CENTRIC,
                build.IndexType.SSM_OCCURRENCE_CENTRIC,
            )
        )
    )

    driver.Driver.run(config)

    for method in patch_build_method.values():
        method.assert_called_once()


@pytest.mark.usefixtures("patch_spark_session")
def test__run__call_only_required_builders(
    patch_build_method: Mapping[
        type[Union[bases.Builder, builders.BaseBuilder]], mock.MagicMock
    ]
) -> None:
    config = mock.MagicMock(
        build=mock.MagicMock(
            index_types=(
                build.IndexType.CNV_CENTRIC,
                build.IndexType.CNV_OCCURRENCE_CENTRIC,
            )
        )
    )
    skipped_builders = frozenset(
        {
            builders.CaseCentricBuilder,
            builders.GeneCentricBuilder,
            builders.SSMOccurrenceCentricBuilder,
            builders.SSMCentricBuilder,
            builders.MAFBuilder,
            builders.PrimaryAliquotBuilder,
            civic.DNABuilder,
            civic.ProteinBuilder,
        }
    )

    driver.Driver.run(config)

    for builder, build_method in patch_build_method.items():
        if builder in skipped_builders:
            build_method.assert_not_called()
        else:
            build_method.assert_called_once()
