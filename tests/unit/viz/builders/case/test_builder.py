from collections.abc import Iterable
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import builders, configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.viz.builders.case.inputs import raw

CASE_ID_SCHEMA = "case_id: string"


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    # Hello
    return schemas.Viz.Builders.Case.RAW.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.FINAL.load()


class TestCaseBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        assert_schemas_equal: utils.AssertSchemasEqual,
        case_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.assert_schemas_equal = assert_schemas_equal
        self.case_schema = case_schema
        self.final_schema = final_schema

    def arrange_config(self) -> configuration.CaseBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=configuration.CaseBuilder,
            projects=(),
            repartition_size=1,
            include_as_arrays=(),
            is_cached=False,
            backup=backup,
        )

    def arrange_es_dataframe_util(
        self, cases: Iterable[raw.Case] = (raw.Case(),)
    ) -> es_utils.DataFrameUtil:
        util = mock.MagicMock(spec=es_utils.DataFrameUtil)

        util.read.return_value = self.spark_session.createDataFrame(
            cases,  # type: ignore
            schema=self.case_schema,
        )

        return util

    def arrange_input_dataframes(
        self,
        maf_metadata_case_ids: Iterable[str] = (),
        ascat_metadata_case_ids: Iterable[str] = (),
        segment_cnv_metadata_ids: Iterable[str] = (),
    ) -> dict[str, sql.DataFrame]:
        def to_rows(case_ids: Iterable[str]) -> tuple[sql.Row, ...]:
            return tuple(sql.Row(case_id=case_id) for case_id in case_ids)

        maf_metadata_df = self.spark_session.createDataFrame(
            to_rows(maf_metadata_case_ids), schema=CASE_ID_SCHEMA
        )
        ascat_metadata_df = self.spark_session.createDataFrame(
            to_rows(ascat_metadata_case_ids), schema=CASE_ID_SCHEMA
        )
        segment_cnv_metadata_df = self.spark_session.createDataFrame(
            to_rows(segment_cnv_metadata_ids), schema=CASE_ID_SCHEMA
        )
        return {
            "maf_metadata_df": maf_metadata_df,
            "ascat_metadata_df": ascat_metadata_df,
            "segment_cnv_metadata_df": segment_cnv_metadata_df,
        }

    def arrange_case_field_selector(self) -> es_utils.CaseFieldSelector:
        selector = mock.MagicMock(spec=es_utils.CaseFieldSelector)
        selector.select_for.return_value = ()

        return selector

    @pytest.mark.case_schema_dependent
    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util()
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1

        self.assert_schemas_equal(
            result_df.schema, self.final_schema, schemas.Viz.Builders.Case.FINAL
        )

    def test__build__data_translated(self) -> None:
        config = self.arrange_config()
        case = raw.Case()
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        case.assert_equals(result_row)

    @pytest.mark.parametrize(
        (
            "maf_metadata_cases",
            "ascat_metadata_cases",
            "segment_cnv_metadata_cases",
            "available_variation_data",
        ),
        (
            ((), (), (), frozenset(())),
            (("case-0",), (), (), frozenset(("ssm",))),
            ((), ("case-0",), (), frozenset(("cnv",))),
            ((), (), ("case-0",), frozenset(("segment_cnv",))),
            (
                ("case-0",),
                ("case-0",),
                ("case-0",),
                frozenset(("cnv", "ssm", "segment_cnv")),
            ),
        ),
        ids=(
            "neither",
            "only-in-metadata",
            "only-in-ascat",
            "only-in-segment-cnv",
            "metadata-and-ascat",
        ),
    )
    def test__build__available_variation_data(
        self,
        maf_metadata_cases: Iterable[str],
        ascat_metadata_cases: Iterable[str],
        segment_cnv_metadata_cases: Iterable[str],
        available_variation_data: frozenset[str],
    ) -> None:
        config = self.arrange_config()
        case = raw.Case(case_id="case-0")
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes(
            maf_metadata_cases, ascat_metadata_cases, segment_cnv_metadata_cases
        )
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert frozenset(result_row.available_variation_data or ()) == available_variation_data
