import contextlib
from collections.abc import Iterable, Iterator
import dataclasses
from unittest import mock
import more_itertools

import deepdiff
import pytest
from pyspark import sql
from pyspark.sql import types

import config
from exports import builders
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def ssm_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.SSM.Occurrence.FINAL.load()


@pytest.fixture(scope="class")
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.SSM.WithoutAAChange.FINAL.load()


@pytest.fixture(scope="class")
def observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.SSM.FINAL.load()


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.FINAL.load()


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.FINAL.load()


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.Viz.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.SSMOccurrenceCentric.FINAL.load()


class TestSSMCentricBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        ssm_schema: types.StructType,
        consequence_schema: types.StructType,
        observation_schema: types.StructType,
        maf_schema: types.StructType,
        case_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.ssm_schema = ssm_schema
        self.consequence_schema = consequence_schema
        self.observation_schema = observation_schema
        self.maf_schema = maf_schema
        self.case_schema = case_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.final_schema = final_schema

    @contextlib.contextmanager
    def arrange_build_ssm_subtree(
        self, ssms: Iterable[models.SSM] = (models.SSM(),)
    ) -> Iterator[None]:
        df = self.create_dataframe(ssms, self.ssm_schema)

        with mock.patch(
            "exports.builders.df_builders.build_ssm_subtree", return_value=df
        ):
            yield

    def arrange_consequence_builder(
        self, consequences: Iterable[models.SSMConsequence] = (models.SSMConsequence(),)
    ) -> builders.ConsequenceBuilder:
        df = self.create_dataframe(consequences, self.consequence_schema)
        builder = mock.MagicMock(spec=builders.ConsequenceBuilder)
        builder.build_for_ssm.return_value = df

        return builder

    def arrange_observation_builder(
        self, observations: Iterable[models.SSMObservation] = (models.SSMObservation(),)
    ) -> builders.ObservationBuilder:
        df = self.create_dataframe(observations, self.observation_schema)
        builder = mock.MagicMock(spec=builders.ObservationBuilder)
        builder.build_for_ssm.return_value = df

        return builder

    def arrange_config(self) -> config.BaseConfig:
        return mock.MagicMock(
            spec=config.BaseConfig,
            output_raw="",
            debug=False,
        )

    def arrange_inputs(
        self,
        mafs: Iterable[models.MAF] = (models.MAF(),),
        cases: Iterable = (models.Case(),),
        primary_aliquots: Iterable[models.PrimaryAliquot] = (models.PrimaryAliquot(),),
    ) -> dict[str, sql.DataFrame]:
        return {
            "maf_df": self.create_dataframe(mafs, self.maf_schema),
            "case_df": self.create_dataframe(cases, self.case_schema),
            "primary_aliquot_df": self.create_dataframe(
                primary_aliquots, self.primary_aliquot_schema
            ),
        }

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        inputs = self.arrange_inputs()

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()

        builder = builders.SSMOccurrenceCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_build_ssm_subtree():
            builder.build(**inputs)

        assert builder.ssm_occurrence_centric
        assert builder.ssm_occurrence_centric.count() == 1
        assert builder.ssm_occurrence_centric.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        ssm = models.SSM()
        ssm_observation = models.SSMObservation(occurrence_id="occ-1")
        case = models.Case()

        config = self.arrange_config()
        inputs = self.arrange_inputs(cases=(case,))

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder((ssm_observation,))

        builder = builders.SSMOccurrenceCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_build_ssm_subtree((ssm,)):
            builder.build(**inputs)

        assert builder.ssm_occurrence_centric

        result_row = more_itertools.one(builder.ssm_occurrence_centric.collect())

        assert result_row.ssm_occurrence_id == ssm_observation.occurrence_id

        result_ssm = result_row.ssm

        assert result_ssm.ssm_id == ssm.ssm_id
        assert result_ssm.chromosome == ssm.chromosome
        assert tuple(result_ssm.cosmic_id) == ssm.cosmic_id
        assert result_ssm.end_position == ssm.end_position
        assert result_ssm.genomic_dna_change == ssm.genomic_dna_change
        assert result_ssm.mutation_subtype == ssm.mutation_subtype
        assert result_ssm.mutation_type == ssm.mutation_type
        assert result_ssm.ncbi_build == ssm.ncbi_build
        assert result_ssm.reference_allele == ssm.reference_allele
        assert result_ssm.start_position == ssm.start_position
        assert result_ssm.tumor_allele == ssm.tumor_allele

        result_clinical_annotations = result_ssm.clinical_annotations
        clinical_annotations = ssm.clinical_annotations
        assert result_clinical_annotations and clinical_annotations
        assert not deepdiff.DeepDiff(
            utils.row_to_dict(result_clinical_annotations),
            dataclasses.asdict(clinical_annotations),
        )

        result_consequence = more_itertools.one(result_ssm.consequence)
        consequence = more_itertools.one(ssm.consequence or ())
        assert not deepdiff.DeepDiff(
            utils.row_to_dict(result_consequence), dataclasses.asdict(consequence)
        )

        result_case = utils.row_to_dict(result_row.case)
        result_observation = more_itertools.one(result_case.pop("observation"))
        observation = more_itertools.one(ssm.observation or ())

        assert not deepdiff.DeepDiff(result_case, dataclasses.asdict(case))
        assert not deepdiff.DeepDiff(
            result_observation, dataclasses.asdict(observation)
        )

    def test__build__cases_are_optional(self) -> None:
        case = models.Case(case_id="case-1")

        config = self.arrange_config()
        inputs = self.arrange_inputs(cases=(case,))

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()

        builder = builders.SSMOccurrenceCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_build_ssm_subtree():
            builder.build(**inputs)

        assert builder.ssm_occurrence_centric

        result_row = more_itertools.one(builder.ssm_occurrence_centric.collect())
        result_case = result_row.case.asDict(recursive=True)
        _ = result_case.pop("observation", None)
        _ = result_case.pop("case_id", None)

        print(result_case)
        assert all(v is None for v in result_case.values())
