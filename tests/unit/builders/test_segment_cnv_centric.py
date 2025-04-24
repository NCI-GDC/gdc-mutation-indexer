"""Test SegmentCNVCentricBuilder dataframe creation."""

import dataclasses
from collections.abc import Iterable
from unittest import mock

import deepdiff
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders import segment_cnv_centric
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
    return schemas.Viz.Builders.SegmentCNVCentric.FINAL.load()


def assert_root_level_transformed(row: sql.Row, segment_cnv: models.SegmentCNV) -> None:
    assert row.segment_cnv_id == segment_cnv.segment_cnv_id
    assert row.chromosome == segment_cnv.chromosome
    assert row.cnv_change == segment_cnv.cnv_change
    assert row.cnv_change_5_category == segment_cnv.cnv_change_5_category
    assert row.start_position == segment_cnv.start_position
    assert row.end_position == segment_cnv.end_position
    assert row.length == segment_cnv.length


def assert_observation_transformed(
    occurrence: sql.Row, segment_cnv: models.SegmentCNV
) -> None:
    observation = more_itertools.one(occurrence.case.observation)

    assert observation.copy_number == segment_cnv.copy_number
    assert observation.observation_id == segment_cnv.observation_id
    assert observation.sample_ploidy_integer == segment_cnv.sample_ploidy_integer
    assert observation.src_file_id == segment_cnv.src_file_id
    assert observation.variant_calling.variant_caller == segment_cnv.variant_caller
    assert observation.variant_status == segment_cnv.variant_status


def assert_case_transformed(occurrence: sql.Row, case: models.Case) -> None:
    final_case = occurrence.case.asDict(recursive=True)
    _ = final_case.pop("observation")
    final_case = utils.convert_lists(final_case)
    expected_case = dataclasses.asdict(case)

    assert not deepdiff.DeepDiff(final_case, expected_case)


def assert_occurrence_transformed(
    row: sql.Row, segment_cnv: models.SegmentCNV, case: models.Case
) -> None:
    occurrence = more_itertools.one(row.occurrence)

    assert occurrence.occurrence_id == segment_cnv.occurrence_id

    assert_observation_transformed(occurrence, segment_cnv)
    assert_case_transformed(occurrence, case)


def assert_occurrence_grouped_transformed(
    row: sql.Row,
    segment_cnvs: tuple[models.SegmentCNV, ...],
    cases: tuple[models.Case, ...],
) -> None:
    assert len(row.occurrence) == 2
    sorted_occurrences_by_obs_id = sorted(
        row.occurrence, key=lambda occ: occ.occurrence_id
    )

    for i, (segment_cnv, case) in enumerate(zip(segment_cnvs, cases)):
        occurrence = sorted_occurrences_by_obs_id[i]
        assert occurrence.occurrence_id == segment_cnv.occurrence_id

        assert_observation_transformed(occurrence, segment_cnv)
        assert_case_transformed(occurrence, case)


class TestSegmentCNVCentricBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        segment_cnv_schema: types.StructType,
        case_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._segment_cnv_schema = segment_cnv_schema
        self._case_schema = case_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> configuration.SegmentCNVCentricBuilder:
        config = mock.MagicMock(
            spec=configuration.SegmentCNVCentricBuilder,
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
            "segment_cnv_df": self._create_dataframe(
                segment_cnvs, self._segment_cnv_schema
            ),
            "case_df": self._create_dataframe(cases, self._case_schema),
        }

    def test__build__single_row(self) -> None:
        """Tests building a single row.

        A baseline test to ensure that the default configuration yields one row and it
        has the expected structure.

        Given a segment_cnv and case dataframes
        When SegmentCNVCentricBuilder build is called
        Then return a dataframe with one result and correct schema.
        """
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs((models.SegmentCNV(),), (models.Case(),))
        builder = segment_cnv_centric.SegmentCNVCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert not deepdiff.DeepDiff(
            result_df.schema, self._final_schema, ignore_order=True
        )

    def test__build__data_transformed(self) -> None:
        """Test the correctness of the output segmetn cnv centric dataframe.

        Given a segment_cnv and case dataframes
        When SegmentCNVCentricBuilder build is called
        Then return a dataframe with the correct segment_cnv_centric dataframe
            and schema.
        """
        segment_cnv, case = models.SegmentCNV(), models.Case()
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = mock.MagicMock()
        mappings_loader = utils.arrange_empty_mappings_loader()
        inputs = self._arrange_inputs((segment_cnv,), (case,))
        builder = segment_cnv_centric.SegmentCNVCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert not deepdiff.DeepDiff(
            result_df.schema, self._final_schema, ignore_order=True
        )
        result_row = more_itertools.one(result_df.collect())

        assert_root_level_transformed(result_row, segment_cnv)
        assert_occurrence_transformed(result_row, segment_cnv, case)

    def test__build__occurrences_grouped(self) -> None:
        """Test correctness of occurrences grouping logic.

        Given segment_cnv dataframe with two rows where they share segment_cnv_id
            but have different file and case ids
        When SegmentCNVCentricBuilder build is called
        Then returned dataframe has one row with the occurrence column having
            two occurrence objects, each with one observation.
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
                occurrence_id="occ-1",
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
        builder = segment_cnv_centric.SegmentCNVCentricBuilder(
            config, spark_session, es_dataframe_util, mappings_loader
        )
        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert not deepdiff.DeepDiff(
            result_df.schema, self._final_schema, ignore_order=True
        )
        result_row = more_itertools.one(result_df.collect())

        assert_root_level_transformed(result_row, segment_cnvs[0])
        assert_occurrence_grouped_transformed(result_row, segment_cnvs, cases)
