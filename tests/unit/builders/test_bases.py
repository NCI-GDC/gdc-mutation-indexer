from typing import AbstractSet, Generic, Mapping, Optional, Type, TypeVar
from unittest import mock

import pytest
from pyspark import sql
from typing_extensions import TypedDict

from exports.builders import bases
from exports.configuration.builders import common
from exports.constants import build

TInputs = TypeVar("TInputs", bound=Mapping[str, object])


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
        ids=("empty", "exact_match", "extra"),
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
        ids=("empty", "dummy"),
    )
    def test__required_dataframes__all_present(
        self, input_type: Type[TypedDict], expected_dfs: AbstractSet[build.DataFrame]
    ) -> None:
        manager = bases.InputDataFrameManger(input_type)

        assert frozenset(manager.required_dataframes) == expected_dfs


class TestInputBuilder:
    class DummyBuilder(Generic[TInputs], bases.InputBuilder[common.Builder, TInputs]):
        def __init__(
            self,
            input_type: Type[TInputs],
            output: build.DataFrame,
            config: Optional[common.Builder],
            spark_session: Optional[sql.SparkSession],
            scratch_df: Optional[sql.DataFrame],
        ) -> None:
            super().__init__(
                config
                or mock.MagicMock(
                    is_cached=False,
                    backup=mock.MagicMock(mode=build.BackupMode.NEITHER),
                ),
                spark_session or mock.MagicMock(),
                input_type,
                output,
            )

            self._scratch_df = scratch_df or mock.MagicMock(
                collect=mock.MagicMock(
                    return_value=(sql.Row(id=self.__class__.__name__),)
                )
            )

        def _build_from_scratch(self, input_dfs: TInputs) -> sql.DataFrame:
            return self._scratch_df

    class Builder0(DummyBuilder[EmptyInputs]):
        def __init__(
            self,
            config: Optional[common.Builder] = None,
            spark_session: Optional[sql.SparkSession] = None,
            scratch_df: Optional[sql.DataFrame] = None,
        ) -> None:
            super().__init__(
                EmptyInputs,
                build.DataFrame.MAF_METADATA,
                config,
                spark_session,
                scratch_df,
            )

    class Builder1(DummyBuilder[DummyInputs]):
        def __init__(
            self,
            output: build.DataFrame = build.DataFrame.GENE_MODEL,
            config: Optional[common.Builder] = None,
            spark_session: Optional[sql.SparkSession] = None,
            scratch_df: Optional[sql.DataFrame] = None,
        ) -> None:
            super().__init__(
                DummyInputs, output, config, spark_session, scratch_df,
            )

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
            (Builder1(), build.DataFrame.GENE_MODEL),
        ),
    )
    def test__output(
        self, builder: bases.InputBuilder, expected_output: build.DataFrame,
    ) -> None:
        assert builder.output == expected_output

    @pytest.mark.parametrize(
        ("builder", "inputs"),
        (
            (Builder0(), {}),
            (
                Builder1(),
                {
                    "case_df": mock.MagicMock(),
                    "maf_df": mock.MagicMock(),
                    "gene_model_df": mock.MagicMock(),
                },
            ),
        ),
        ids=("empty", "dummy"),
    )
    def test__build__all_inputs_given(
        self, builder: bases.Builder, inputs: Mapping[str, sql.DataFrame],
    ) -> None:
        result_df = builder.build(**inputs)
        result_rows = result_df.collect()

        assert len(result_rows) == 1
        assert result_rows[0].id == builder.__class__.__name__

    def test__build__missing_inputs_raises(self) -> None:
        builder = TestInputBuilder.Builder1()
        inputs = {"maf_df": mock.MagicMock()}

        with pytest.raises(AssertionError, match=r"Missing required inputs\."):
            builder.build(**inputs)

    def test__build__backup_both(self) -> None:
        df = mock.MagicMock(spec=sql.DataFrame, name="scratch_df")
        df.write.parquet = mock.MagicMock(return_value=None)
        read_df = mock.MagicMock(spec=sql.DataFrame, name="read_df")
        read_df.write.parquet = mock.MagicMock(return_value=None)
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.parquet = mock.MagicMock(side_effect=(read_df, df))
        config = mock.MagicMock(
            spec=common.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.BOTH, path="test/path"),
        )
        builder = TestInputBuilder.Builder0(config, spark_session, scratch_df=df)

        result_df = builder.build()

        assert result_df is read_df
        read_df.write.parquet.assert_not_called()
        df.write.parquet.assert_called_once_with(config.backup.path, mode="overwrite")
        spark_session.read.parquet.assert_called_once_with(config.backup.path)

    def test__build__backup_read(self) -> None:
        df = mock.MagicMock(spec=sql.DataFrame)
        df.write.parquet = mock.MagicMock(return_value=None)
        read_df = mock.MagicMock(spec=sql.DataFrame)
        read_df.write.parquet = mock.MagicMock(return_value=None)
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.parquet = mock.MagicMock(side_effect=(read_df, df))
        config = mock.MagicMock(
            spec=common.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.READ, path="test/path"),
        )
        builder = TestInputBuilder.Builder0(config, spark_session, scratch_df=df)

        result_df = builder.build()

        assert result_df is read_df
        read_df.write.parquet.assert_not_called()
        df.write.parquet.assert_not_called()
        spark_session.read.parquet.assert_called_once_with(config.backup.path)

    def test__build__backup_write(self) -> None:
        df = mock.MagicMock(spec=sql.DataFrame)
        df.write.parquet = mock.MagicMock(return_value=None)
        read_df = mock.MagicMock(spec=sql.DataFrame)
        read_df.write.parquet = mock.MagicMock(return_value=None)
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.parquet = mock.MagicMock(side_effect=(read_df, df))
        config = mock.MagicMock(
            spec=common.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.WRITE, path="test/path"),
        )
        builder = TestInputBuilder.Builder0(config, spark_session, scratch_df=df)

        result_df = builder.build()

        assert result_df is df
        df.write.parquet.assert_called_once_with(config.backup.path, mode="overwrite")
        spark_session.read.parquet.assert_not_called()

    def test__build__backup_neither(self) -> None:
        df = mock.MagicMock(spec=sql.DataFrame)
        df.write.parquet = mock.MagicMock(return_value=None)
        read_df = mock.MagicMock(spec=sql.DataFrame)
        read_df.write.parquet = mock.MagicMock(return_value=None)
        spark_session = mock.MagicMock(spec=sql.SparkSession)
        spark_session.read.parquet = mock.MagicMock(side_effect=(read_df, df))
        config = mock.MagicMock(
            spec=common.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path="test/path"),
        )
        builder = TestInputBuilder.Builder0(config, spark_session, scratch_df=df)

        result_df = builder.build()

        assert result_df is df
        df.write.parquet.assert_not_called()
        spark_session.read.parquet.assert_not_called()

    @pytest.mark.parametrize(
        "is_cached", (True, False), ids=("is_cached", "is_not_cached")
    )
    def test__build__caching(self, is_cached: bool) -> None:
        cached_df = mock.MagicMock(spec=sql.DataFrame)
        df = mock.MagicMock(spec=sql.DataFrame)
        df.cache.return_value = cached_df
        expected_df = cached_df if is_cached else df
        config = mock.MagicMock(
            spec=common.Builder,
            is_cached=is_cached,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER),
        )
        builder = TestInputBuilder.Builder0(config, scratch_df=df)

        result_df = builder.build()

        assert result_df is expected_df

    def test__hash__hash_is_same_as_output(self) -> None:
        builder = TestInputBuilder.Builder0()

        assert (
            hash(builder) == hash(builder.output) == hash(build.DataFrame.MAF_METADATA)
        )

    def test__eq__compare_with_self_true(self) -> None:
        builder = TestInputBuilder.Builder0()

        assert builder == builder

    def test__eq__builders_with_same_output_true(self) -> None:
        builder0 = TestInputBuilder.Builder0()
        builder1 = TestInputBuilder.Builder1(output=builder0.output)

        assert builder0 == builder1

    def test__eq__same_builder_output_and_dataframe_true(self) -> None:
        builder = TestInputBuilder.Builder0()

        assert builder == build.DataFrame.MAF_METADATA

    def test__eq__builders_with_diff_outputs_false(self) -> None:
        builder0 = TestInputBuilder.Builder0()
        builder1 = TestInputBuilder.Builder1()

        assert builder0 != builder1

    def test__eq__diff_builder_output_and_dataframe_false(self) -> None:
        builder = TestInputBuilder.Builder1()

        assert builder != build.DataFrame.MAF_METADATA
