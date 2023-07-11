import contextlib
import dataclasses
from collections.abc import Iterable, Iterator
from unittest import mock

import deepdiff
import more_itertools
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
    return schemas.Viz.Builders.DFBuilders.SSM.FINAL.load()


@pytest.fixture(scope="class")
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.SSM.FINAL.load()


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
    return schemas.Viz.Builders.SSMCentric.FINAL.load()


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
    def arrange_get_ssm_df(
        self, ssms: Iterable[models.SSM] = (models.SSM(),)
    ) -> Iterator[None]:
        df = self.create_dataframe(ssms, self.ssm_schema)

        with mock.patch("exports.builders.df_builders.get_ssm_df", return_value=df):
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
            percentile_threshold={"occurrences_per_ssm": 100},
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

        builder = builders.SSMCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_get_ssm_df():
            builder.build(**inputs)

        assert builder.ssm_centric
        assert builder.ssm_centric.count() == 1
        assert builder.ssm_centric.schema == self.final_schema

    def test__build__data_transformed(self) -> None:
        ssm = models.SSM()
        ssm_consequence = models.SSMConsequence()
        ssm_observation = models.SSMObservation()
        case = models.Case()

        config = self.arrange_config()
        inputs = self.arrange_inputs(cases=(case,))

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder((ssm_consequence,))
        observation_builder = self.arrange_observation_builder((ssm_observation,))

        builder = builders.SSMCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_get_ssm_df((ssm,)):
            builder.build(**inputs)

        assert builder.ssm_centric

        result_row = more_itertools.one(builder.ssm_centric.collect())
        assert result_row.ssm_id == ssm.ssm_id
        assert result_row.chromosome == ssm.chromosome
        assert tuple(result_row.cosmic_id) == ssm.cosmic_id
        assert result_row.end_position == ssm.end_position
        assert result_row.genomic_dna_change == ssm.genomic_dna_change
        assert result_row.mutation_subtype == ssm.mutation_subtype
        assert result_row.mutation_type == ssm.mutation_type
        assert result_row.ncbi_build == ssm.ncbi_build
        assert result_row.reference_allele == ssm.reference_allele
        assert result_row.start_position == ssm.start_position
        assert result_row.tumor_allele == ssm.tumor_allele

        result_clinical_annotations = result_row.clinical_annotations
        clinical_annotations = ssm.clinical_annotations
        assert result_clinical_annotations and clinical_annotations

        result_civic = result_clinical_annotations.civic
        civic = clinical_annotations.civic
        assert result_civic and civic
        assert result_civic.gene_id == civic.gene_id
        assert result_civic.variant_id == civic.variant_id

        assert tuple(result_row.gene_aa_change) == ssm_consequence.gene_aa_change

        result_consequence = more_itertools.one(result_row.consequence)
        consequence = more_itertools.one(ssm_consequence.consequence or ())
        assert result_consequence and consequence
        assert not deepdiff.DeepDiff(
            utils.row_to_dict(result_consequence), dataclasses.asdict(consequence)
        )

        result_occurrence = more_itertools.one(result_row.occurrence)
        assert result_occurrence.occurrence_id == ssm_observation.occurrence_id

        result_case = utils.row_to_dict(result_occurrence.case)
        result_observations = result_case.pop("observation", None)
        observation = more_itertools.one(ssm_observation.observation or ())

        assert result_observations and observation
        assert not deepdiff.DeepDiff(result_case, dataclasses.asdict(case))
        assert not deepdiff.DeepDiff(
            more_itertools.one(result_observations), dataclasses.asdict(observation)
        )

    def test__build__cases_are_optional(self) -> None:
        case = models.Case(case_id="case-1")
        observation = models.SSMObservation(case_id="case-0")

        config = self.arrange_config()
        inputs = self.arrange_inputs(cases=(case,))

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            observations=(observation,)
        )

        builder = builders.SSMCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_get_ssm_df():
            builder.build(**inputs)

        assert builder.ssm_centric

        result_row = more_itertools.one(builder.ssm_centric.collect())
        result_occurrence = more_itertools.one(result_row.occurrence)
        result_case = result_occurrence.case.asDict(recursive=True)
        _ = result_case.pop("observation", None)
        _ = result_case.pop("case_id", None)

        print(result_case)
        assert all(v is None for v in result_case.values())

    def test__build__observations_with_same_ssm_grouped(self) -> None:
        observation0 = models.SSMObservation()
        observation1 = models.SSMObservation()

        config = self.arrange_config()
        inputs = self.arrange_inputs()

        sql_context = mock.MagicMock()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            observations=(observation0, observation1)
        )

        builder = builders.SSMCentricBuilder(
            config, sql_context, consequence_builder, observation_builder
        )

        with self.arrange_get_ssm_df():
            builder.build(**inputs)

        assert builder.ssm_centric

        result_row = more_itertools.one(builder.ssm_centric.collect())

        assert len(result_row.occurrence) == 2
