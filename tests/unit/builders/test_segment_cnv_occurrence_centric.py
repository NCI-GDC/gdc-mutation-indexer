"""Test SegmentCNVOccurrenceCentric dataframe creation."""

import dataclasses
from collections.abc import Iterable
from unittest import mock

import more_itertools
import pytest
from deepdiff import diff
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders import segment_cnv_occurrence_centric
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def segment_cnv_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNV.FINAL.load()


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNVOccurrenceCentric.FINAL.load()


def assert_segment_cnv_transformed(
    segment_cnv_data: sql.Row, segment_cnv: models.SegmentCNV
) -> None:
    assert segment_cnv_data.segment_cnv_id == segment_cnv.segment_cnv_id
    assert segment_cnv_data.chromosome == segment_cnv.chromosome
    assert segment_cnv_data.cnv_change == segment_cnv.cnv_change
    assert segment_cnv_data.cnv_change_5_category == segment_cnv.cnv_change_5_category
    assert segment_cnv_data.start_position == segment_cnv.start_position
    assert segment_cnv_data.end_position == segment_cnv.end_position
    assert segment_cnv_data.length == segment_cnv.length


def assert_observation_transformed(
    observation: sql.Row, segment_cnv: models.SegmentCNV
) -> None:
    assert observation.copy_number == segment_cnv.copy_number
    assert observation.observation_id == segment_cnv.observation_id
    assert observation.sample_ploidy_integer == segment_cnv.sample_ploidy_integer
    assert observation.src_file_id == segment_cnv.src_file_id
    assert observation.variant_calling.variant_caller == segment_cnv.variant_caller
    assert observation.variant_status == segment_cnv.variant_status


def assert_case_transformed(case_data: sql.Row, case: models.Case) -> None:
    final_case = case_data.asDict(recursive=True)
    _ = final_case.pop("observation")
    final_case = utils.convert_lists(final_case)
    expected_case = dataclasses.asdict(case)

    assert not diff.DeepDiff(final_case, expected_case)


def assert_observation_grouped_transformed(
    row: sql.Row,
    segment_cnvs: tuple[models.SegmentCNV, ...],
    case: models.Case,
) -> None:
    observations = row.case.observation
    assert len(observations) == 2

    sorted_observations = sorted(observations, key=lambda obs: obs.observation_id)

    for observation, segment_cnv in zip(sorted_observations, segment_cnvs):
        assert_observation_transformed(observation, segment_cnv)

    assert_case_transformed(row.case, case)


class TestSegmentCNVOccurrenceCentricBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        assert_schemas_equal: utils.AssertSchemasEqual,
        segment_cnv_schema: types.StructType,
        case_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._assert_schemas_equal = assert_schemas_equal
        self._segment_cnv_schema = segment_cnv_schema
        self._case_schema = case_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> configuration.SegmentCNVOccurrenceCentricBuilder:
        config = mock.MagicMock(
            spec=configuration.SegmentCNVOccurrenceCentricBuilder,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            acl=(),
            partition_size=1,
            id_field="segment_cnv_id",
        )

        return config

    def _arrange_inputs(
        self, segment_cnvs: Iterable[models.SegmentCNV], cases: Iterable[models.Case]
    ) -> dict:
        return {
            "segment_cnv_df": self._create_dataframe(segment_cnvs, self._segment_cnv_schema),
            "case_df": self._create_dataframe(cases, self._case_schema),
        }

    def test__build__single_row(self) -> None:
        """Tests building a single row.

        A baseline test to ensure that the default configuration yields one row and it
        has the expected structure.

        Given a segment_cnv and case dataframes
        When SegmentCNVOccurrenceCentricBuilder build is called
        Then return a dataframe with one result and correct schema.
        """
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs((models.SegmentCNV(),), (models.Case(),))
        builder = segment_cnv_occurrence_centric.SegmentCNVOccurrenceCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1

        self._assert_schemas_equal(
            result_df.schema,
            self._final_schema,
            schemas.Viz.Builders.SegmentCNVOccurrenceCentric.FINAL,
        )

    @pytest.mark.case_schema_dependent
    def test__build__data_transformed(self) -> None:
        """Test the correctness of the output segment cnv centric dataframe.

        Given a segment_cnv and case dataframes
        When SegmentCNVOccurrenceCentricBuilder build is called
        Then return a dataframe with the correct segment_cnv_occurrence_centric
            dataframe and schema.
        """
        segment_cnv, case = models.SegmentCNV(), models.Case()
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs((segment_cnv,), (case,))
        builder = segment_cnv_occurrence_centric.SegmentCNVOccurrenceCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        result_row = more_itertools.one(result_df.collect())

        assert result_row.segment_cnv_occurrence_id == segment_cnv.occurrence_id
        assert_segment_cnv_transformed(result_row.segment_cnv, segment_cnv)
        assert_case_transformed(result_row.case, case)
        assert_observation_transformed(
            more_itertools.one(result_row.case.observation), segment_cnv
        )

    def test__build__observations_grouped(self) -> None:
        """Test correctness of grouping observations.

        Given segment_cnv dataframe with two rows where they share segment_cnv_id
            and case_id but have different file_id
        When SegmentCNVOccurrenceCentricBuilder build is called
        Then returned dataframe has one row with case object
            having two observations.
        """
        segment_cnvs = (
            models.SegmentCNV(
                segment_cnv_id="segment_cnv-id-0",
                occurrence_id="occ-0",
                observation_id="obs-0",
                case_id="case-0",
                aliquot_id="aliquot-0",
            ),
            models.SegmentCNV(
                segment_cnv_id="segment_cnv-id-0",
                occurrence_id="occ-0",
                observation_id="obs-1",
                case_id="case-0",
                aliquot_id="aliquot-1",
            ),
        )
        case = models.Case(case_id="case-0")
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs(segment_cnvs, (case,))
        builder = segment_cnv_occurrence_centric.SegmentCNVOccurrenceCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        result_row = more_itertools.one(result_df.collect())

        assert result_row.segment_cnv_occurrence_id == segment_cnvs[0].occurrence_id
        assert_segment_cnv_transformed(result_row.segment_cnv, segment_cnvs[0])
        assert_observation_grouped_transformed(result_row, segment_cnvs, case)

    def test__build__multiple_rows(self) -> None:
        """Test correctness of each row being unique on (segment_cnv_id, case_id).

        Given segment_cnv dataframe with two rows where they share segment_cnv_id
            but have different case_id and file_id
        When SegmentCNVOccurrenceCentricBuilder build is called
        Then returned dataframe has two rows with each row having
            one observation.
        """
        segment_cnvs = (
            models.SegmentCNV(
                segment_cnv_id="segment_cnv-id-0",
                occurrence_id="occ-0",
                observation_id="obs-0",
                case_id="case-0",
                aliquot_id="aliquot-0",
            ),
            models.SegmentCNV(
                segment_cnv_id="segment_cnv-id-0",
                occurrence_id="occ-0",
                observation_id="obs-1",
                case_id="case-1",
                aliquot_id="aliquot-1",
            ),
        )
        cases = (models.Case(case_id="case-0"), models.Case(case_id="case-1"))
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs(segment_cnvs, cases)
        builder = segment_cnv_occurrence_centric.SegmentCNVOccurrenceCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 2
        result_rows = result_df.collect()
        sorted_results = sorted(result_rows, key=lambda row: row.segment_cnv.segment_cnv_id)

        for row, segment_cnv, case in zip(sorted_results, segment_cnvs, cases):
            assert row.segment_cnv_occurrence_id == segment_cnv.occurrence_id
            assert_segment_cnv_transformed(row.segment_cnv, segment_cnv)
            assert_observation_transformed(
                more_itertools.one(row.case.observation), segment_cnv
            )
            assert_case_transformed(row.case, case)
