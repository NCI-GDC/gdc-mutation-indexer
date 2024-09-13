import dataclasses
from collections.abc import Iterable
from unittest import mock

import deepdiff
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.configuration import adapter
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.FINAL.load()


@pytest.fixture(scope="class")
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.CNV.FINAL.load()


@pytest.fixture(scope="class")
def observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.CNV.CNV.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.CNVCentric.FINAL.load()


def assert_cnv_transformed(row: sql.Row, ascat: models.ASCAT) -> None:
    assert row.cnv_id == ascat.cnv_id
    assert row.chromosome == ascat.chromosome
    assert row.cnv_change == ascat.cnv_change
    assert row.end_position == ascat.end_position
    assert row.gene_level_cn == ascat.gene_level_cn
    assert row.ncbi_build == ascat.ncbi_build
    assert row.start_position == ascat.start_position


def assert_consequence_transformed(row: sql.Row, ascat: models.ASCAT) -> None:
    consequence = more_itertools.one(row.consequence)

    assert consequence.consequence_id == ascat.consequence_id
    assert consequence.gene.biotype == ascat.biotype
    assert consequence.gene.gene_id == ascat.gene_id
    assert consequence.gene.is_cancer_gene_census == ascat.is_cancer_gene_census
    assert consequence.gene.symbol == ascat.symbol


def assert_observation_transformed(occurrence: sql.Row, ascat: models.ASCAT) -> None:
    observation = more_itertools.one(occurrence.case.observation)

    assert observation.observation_id == ascat.observation_id
    assert observation.src_file_id == ascat.src_file_id
    assert observation.variant_calling.variant_caller == ascat.variant_caller
    assert observation.variant_status == ascat.variant_status


def assert_case_transformed(occurrence: sql.Row, case: models.Case) -> None:
    final_case = occurrence.case.asDict(recursive=True)
    _ = final_case.pop("observation")
    final_case = utils.convert_lists(final_case)
    expected_case = dataclasses.asdict(case)

    assert not deepdiff.DeepDiff(final_case, expected_case)


def assert_occurrence_transformed(
    row: sql.Row, ascat: models.ASCAT, case: models.Case
) -> None:
    occurrence = more_itertools.one(row.occurrence)

    assert occurrence.occurrence_id == ascat.occurrence_id

    assert_observation_transformed(occurrence, ascat)
    assert_case_transformed(occurrence, case)


class TestCNVCentricBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        ascat_schema: types.StructType,
        case_schema: types.StructType,
        consequence_schema: types.StructType,
        observation_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._ascat_schema = ascat_schema
        self._case_schema = case_schema
        self._consequence_schema = consequence_schema
        self._observation_schema = observation_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> adapter.ObsoleteConfig:
        return mock.MagicMock(
            spec=adapter.ObsoleteConfig,
            percentile_threshold={"occurrences_per_cnv": 100},
            output_raw="",
        )

    def _arrange_consequence_builder(
        self, consequences: Iterable[models.CNVConsequence] = (models.CNVConsequence(),)
    ) -> builders.ConsequenceBuilder:
        builder = mock.MagicMock(spec=builders.ConsequenceBuilder)

        builder.build_for_cnv.return_value = self._create_dataframe(
            consequences, self._consequence_schema
        )

        return builder

    def _arrange_observation_builder(
        self, observations: Iterable[models.CNVObservation] = (models.CNVObservation(),)
    ) -> builders.ObservationBuilder:
        builder = mock.MagicMock(spec=builders.ObservationBuilder)

        builder.build_for_cnv.return_value = self._create_dataframe(
            observations, self._observation_schema
        )

        return builder

    def _arrange_inputs(
        self,
        ascats: Iterable[models.ASCAT] = (models.ASCAT(),),
        cases: Iterable[models.Case] = (models.Case(),),
    ) -> dict:
        return {
            "ascat_df": self._create_dataframe(ascats, self._ascat_schema),
            "case_df": self._create_dataframe(cases, self._case_schema),
        }

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        consequence_builder = self._arrange_consequence_builder()
        observation_builder = self._arrange_observation_builder()
        inputs = self._arrange_inputs()

        builder = builders.CNVCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).cnv_centric

        assert result_df and isinstance(result_df, sql.DataFrame)
        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__data_transformed(self) -> None:
        ascat = models.ASCAT()
        case = models.Case()
        consequence = models.CNVConsequence(
            cnv_id=ascat.cnv_id,
            consequence=(
                models.CNVConsequence.Consequence(
                    consequence_id=ascat.consequence_id,
                    gene=models.CNVConsequence.Consequence.Gene(
                        biotype=ascat.biotype,
                        gene_id=ascat.gene_id,
                        is_cancer_gene_census=ascat.is_cancer_gene_census,
                        symbol=ascat.symbol,
                    ),
                ),
            ),
        )
        observation = models.CNVObservation(
            cnv_id=ascat.cnv_id,
            case_id=ascat.case_id,
            occurrence_id=ascat.occurrence_id,
            observation=(
                models.CNVObservation.Observation(
                    observation_id=ascat.observation_id,
                    src_file_id=ascat.src_file_id,
                    variant_calling=models.CNVObservation.Observation.VariantCalling(
                        variant_caller=ascat.variant_caller
                    ),
                    variant_status=ascat.variant_status,
                ),
            ),
        )

        config = self._arrange_config()
        consequence_builder = self._arrange_consequence_builder(
            consequences=(consequence,)
        )
        observation_builder = self._arrange_observation_builder(
            observations=(observation,)
        )
        inputs = self._arrange_inputs(ascats=(ascat,), cases=(case,))

        builder = builders.CNVCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).cnv_centric

        assert result_df and isinstance(result_df, sql.DataFrame)

        result_row = more_itertools.one(result_df.collect())

        assert_cnv_transformed(result_row, ascat)
        assert_consequence_transformed(result_row, ascat)
        assert_occurrence_transformed(result_row, ascat, case)

    def test__build__occurrences_grouped(self) -> None:
        ascat = models.ASCAT()
        observations = (
            models.CNVObservation(cnv_id=ascat.cnv_id, occurrence_id="occ-0"),
            models.CNVObservation(cnv_id=ascat.cnv_id, occurrence_id="occ-1"),
        )
        config = self._arrange_config()
        consequence_builder = self._arrange_consequence_builder()
        observation_builder = self._arrange_observation_builder(
            observations=observations
        )
        inputs = self._arrange_inputs()

        builder = builders.CNVCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).cnv_centric

        assert result_df and isinstance(result_df, sql.DataFrame)

        result_row = more_itertools.one(result_df.collect())

        assert len(result_row.occurrence) == 2
