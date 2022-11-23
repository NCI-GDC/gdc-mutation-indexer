from typing import AbstractSet
from unittest import mock

import pytest
from pyspark import sql

from exports.builders import bases
from exports.constants import build


class TestInputBuilder:
    class Builder0(bases.InputBuilder[mock.MagicMock]):
        def __init__(self) -> None:
            super().__init__(
                mock.MagicMock(), mock.MagicMock(), build.DataFrame.MAF_METADATA
            )

        def _build_from_scratch(self, **_: sql.DataFrame) -> sql.DataFrame:
            return mock.MagicMock()

    class Builder1(bases.InputBuilder[mock.MagicMock]):
        def __init__(self) -> None:
            super().__init__(
                mock.MagicMock(), mock.MagicMock(), build.DataFrame.EXPRESSION_VALUE
            )

        def _build_from_scratch(
            self,
            case_df: sql.DataFrame,
            maf_df: sql.DataFrame,
            gene_model_df: sql.DataFrame,
            **_: sql.DataFrame
        ) -> sql.DataFrame:
            return mock.MagicMock()

    @pytest.mark.parametrize(
        ("builder", "expected_inputs"),
        (
            (Builder0(), frozenset()),
            (
                Builder1(),
                frozenset(
                    (
                        build.DataFrame.CASE,
                        build.DataFrame.MAF,
                        build.DataFrame.GENE_MODEL,
                    )
                ),
            ),
        ),
    )
    def test__inputs(
        self,
        builder: bases.InputBuilder,
        expected_inputs: AbstractSet[build.DataFrame],
    ) -> None:
        assert frozenset(builder.inputs) == expected_inputs

    @pytest.mark.parametrize(
        ("builder", "expected_output"),
        (
            (Builder0(), build.DataFrame.MAF_METADATA),
            (Builder1(), build.DataFrame.EXPRESSION_VALUE),
        ),
    )
    def test__output(
        self,
        builder: bases.InputBuilder,
        expected_output: build.DataFrame,
    ) -> None:
        assert builder.output == expected_output
