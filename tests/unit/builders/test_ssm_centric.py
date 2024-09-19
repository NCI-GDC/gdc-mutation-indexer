from collections.abc import Iterable
from unittest import mock

import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.configuration import adapter
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


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
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.SSM.FINAL.load()


@pytest.fixture(scope="class")
def observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.SSM.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.SSMCentric.FINAL.load()


class TestSSMCentric:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        maf_schema: types.StructType,
        case_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        consequence_schema: types.StructType,
        observation_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._maf_schema = maf_schema
        self._case_schema = case_schema
        self._primary_aliquot_schema = primary_aliquot_schema
        self._consequence_schema = consequence_schema
        self._observation_schema = observation_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> adapter.ObsoleteConfig:
        return mock.MagicMock(
            spec=adapter.ObsoleteConfig,
            percentile_threshold={"occurrences_per_ssm": 100},
            output_raw="",
        )

    def _arrange_consequence_builder(
        self, consequences: Iterable[models.SSMConsequence] = (models.SSMConsequence(),)
    ) -> builders.ConsequenceBuilder:
        builder = mock.MagicMock(spec=builders.ConsequenceBuilder)

        builder.build_for_ssm.return_value = self._create_dataframe(
            consequences, self._consequence_schema
        )

        return builder

    def _arrange_observation_builder(
        self, observations: Iterable[models.SSMObservation] = (models.SSMObservation(),)
    ) -> builders.ObservationBuilder:
        builder = mock.MagicMock(spec=builders.ObservationBuilder)

        builder.build_for_ssm.return_value = self._create_dataframe(
            observations, self._observation_schema
        )

        return builder

    def _arrange_inputs(
        self,
        mafs: Iterable[models.MAF] = (models.MAF(),),
        cases: Iterable[models.Case] = (models.Case(),),
        primary_aliquots: Iterable[models.PrimaryAliquot] = (models.PrimaryAliquot(),),
    ) -> dict[str, sql.DataFrame]:
        return {
            "maf_df": self._create_dataframe(mafs, self._maf_schema),
            "case_df": self._create_dataframe(cases, self._case_schema),
            "primary_aliquot_df": self._create_dataframe(
                primary_aliquots, self._primary_aliquot_schema
            ),
        }

    def test__build__single_row(self) -> None:
        """Tests a singular row.

        A baseline test to insure that the default configuration yields one row and it
        has the expected structure.
        """
        config = self._arrange_config()
        consequence_builder = self._arrange_consequence_builder()
        observation_builder = self._arrange_observation_builder()
        inputs = self._arrange_inputs()

        builder = builders.SSMCentricBuilder(
            config, mock.MagicMock(), consequence_builder, observation_builder
        )

        result_df = builder.build(**inputs).ssm_centric

        assert result_df and isinstance(result_df, sql.DataFrame)
        assert result_df.count() == 1
        assert result_df.schema == self._final_schema
