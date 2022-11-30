from typing import AbstractSet, Type
from unittest import mock

import pytest
from pyspark import sql
from typing_extensions import TypedDict

from exports.builders import bases
from exports.constants import build


class EmptyInputs(TypedDict):
    pass


class DummyInputs(TypedDict):
    case_df: sql.DataFrame
    maf_df: sql.DataFrame
    gene_model_df: sql.DataFrame


class TestDataFrameInputManager:
    def test__init__all_inputs_must_be_dataframes(self) -> None:
        class BadInputs(TypedDict):
            case_df: sql.DataFrame
            maf_df: str

        with pytest.raises(AssertionError):
            bases.InputDataFrameManger(BadInputs)

    @pytest.mark.parametrize(
        ("input", "input_type"),
        (
            ({}, EmptyInputs),
            (
                {
                    "case_df": mock.MagicMock(),
                    "maf_df": mock.MagicMock(),
                    "gene_model_df": mock.MagicMock(),
                },
                DummyInputs,
            ),
            ({"extra_df": mock.MagicMock()}, EmptyInputs),
        ),
        ids=("empty", "exact_match", "extra")
    )
    def test__check__all_keys_are_contained(
        self, input: dict, input_type: Type[TypedDict]
    ) -> None:
        manager = bases.InputDataFrameManger(input_type)

        assert manager.check(input)

    def test__check__not_all_keys_are_contained(self) -> None:
        manager = bases.InputDataFrameManger(DummyInputs)

        assert not manager.check({"maf_df": mock.MagicMock()})

    @pytest.mark.parametrize(
        ("input_type", "expected_dfs"),
        (
            (EmptyInputs, frozenset()),
            (
                DummyInputs,
                frozenset(
                    (
                        build.DataFrame.CASE,
                        build.DataFrame.MAF,
                        build.DataFrame.GENE_MODEL,
                    )
                ),
            ),
        ),
        ids=("empty", "dataframes")
    )
    def test__required_dataframes__all_present(
        self, input_type: Type[TypedDict], expected_dfs: AbstractSet[build.DataFrame]
    ) -> None:
        manager = bases.InputDataFrameManger(input_type)

        assert frozenset(manager.required_dataframes) == expected_dfs


class TestInputBuilder:
    class Builder0(bases.InputBuilder[mock.MagicMock, EmptyInputs]):
        def __init__(self) -> None:
            super().__init__(
                mock.MagicMock(),
                mock.MagicMock(),
                EmptyInputs,
                build.DataFrame.MAF_METADATA,
            )

        def _build_from_scratch(self, input_dfs: EmptyInputs) -> sql.DataFrame:
            return mock.MagicMock()

    class Builder1(bases.InputBuilder[mock.MagicMock, DummyInputs]):
        def __init__(self) -> None:
            super().__init__(
                mock.MagicMock(),
                mock.MagicMock(),
                DummyInputs,
                build.DataFrame.EXPRESSION_VALUE,
            )

        def _build_from_scratch(self, input_dfs: DummyInputs) -> sql.DataFrame:
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
