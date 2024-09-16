import dataclasses
import unittest
from collections.abc import Iterable
from unittest import mock

import deepdiff
import more_itertools
from pyspark import sql

from mutation_indexer import builders
from mutation_indexer.configuration import adapter
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


def assert_cnv_transformed(row: sql.Row, ascat: models.ASCAT) -> None:
    cnv = row.cnv

    assert cnv.cnv_id == ascat.cnv_id
    assert cnv.chromosome == ascat.chromosome
    assert cnv.cnv_change == ascat.cnv_change
    assert cnv.end_position == ascat.end_position
    assert cnv.gene_level_cn == ascat.gene_level_cn
    assert cnv.ncbi_build == ascat.ncbi_build
    assert cnv.start_position == ascat.start_position


def assert_consequence_transformed(row: sql.Row, ascat: models.ASCAT) -> None:
    consequence = more_itertools.one(row.cnv.consequence)

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
    assert row.cnv_occurrence_id == ascat.occurrence_id

    assert_observation_transformed(row, ascat)
    assert_case_transformed(row, case)


class TestCNVOccurrenceCentricBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._ascat_schema = schemas.Viz.Builders.ASCAT.FINAL.load()
        cls._case_schema = schemas.Viz.Builders.Case.FINAL.load()
        cls._consequence_schema = schemas.Viz.Builders.Consequence.CNV.FINAL.load()
        cls._observation_schema = schemas.Viz.Builders.Observation.CNV.CNV.FINAL.load()
        cls._final_schema = schemas.Viz.Builders.CNVOccurrenceCentric.FINAL.load()

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

        builder.build_for_cnv.return_value = utils.create_dataframe(
            consequences, self._consequence_schema
        )

        return builder

    def _arrange_observation_builder(
        self, observations: Iterable[models.CNVObservation] = (models.CNVObservation(),)
    ) -> builders.ObservationBuilder:
        builder = mock.MagicMock(spec=builders.ObservationBuilder)

        builder.build_for_cnv.return_value = utils.create_dataframe(
            observations, self._observation_schema
        )

        return builder

    def _arrange_inputs(
        self,
        ascats: Iterable[models.ASCAT] = (models.ASCAT(),),
        cases: Iterable[models.Case] = (models.Case(),),
    ) -> dict:
        return {
            "ascat_df": utils.create_dataframe(ascats, self._ascat_schema),
            "case_df": utils.create_dataframe(cases, self._case_schema),
        }

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        consequence_builder = self._arrange_consequence_builder()
        observation_builder = self._arrange_observation_builder()
        inputs = self._arrange_inputs()

        builder = builders.CNVOccurrenceCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).cnv_occurrence_centric

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

        builder = builders.CNVOccurrenceCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).cnv_occurrence_centric

        assert result_df and isinstance(result_df, sql.DataFrame)

        result_row = more_itertools.one(result_df.collect())

        assert_cnv_transformed(result_row, ascat)
        assert_consequence_transformed(result_row, ascat)
        assert_occurrence_transformed(result_row, ascat, case)
