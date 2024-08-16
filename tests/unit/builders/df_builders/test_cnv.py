from collections.abc import Iterable

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.builders import df_builders
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


@pytest.fixture(scope="class")
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.CNV.FINAL.load()


@pytest.fixture(scope="class")
def observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.CNV.Other.FINAL.load()


@pytest.fixture(scope="class")
def final_occurrence_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.CNV.Occurrence.FINAL.load()


@pytest.fixture(scope="class")
def final_other_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.CNV.Other.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.CNV.FINAL.load()


class TestGetCNVDataFrame:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        ascat_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.ascat_schema = ascat_schema
        self.final_schema = final_schema

    def arrange_ascat_df(
        self, ascats: Iterable[models.ASCAT] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return self.create_dataframe(ascats, self.ascat_schema)

    def test__single_row(self) -> None:
        ascat = models.ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))

        result_df = df_builders.get_cnv_df(ascat_df, "cnv_centric")

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.chromosome == ascat.chromosome
        assert result_row.cnv_change == ascat.cnv_change
        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.end_position == ascat.end_position
        assert result_row.gene_level_cn == ascat.gene_level_cn
        assert result_row.ncbi_build == ascat.ncbi_build
        assert result_row.start_position == ascat.start_position


class TestBuildCNVSubtree:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        ascat_schema: types.StructType,
        consequence_schema: types.StructType,
        observation_schema: types.StructType,
        final_occurrence_schema: types.StructType,
        final_other_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.ascat_schema = ascat_schema
        self.consequence_schema = consequence_schema
        self.observation_schema = observation_schema
        self.final_occurrence_schema = final_occurrence_schema
        self.final_other_schema = final_other_schema

    def arrange_ascat_df(
        self, ascats: Iterable[models.ASCAT] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return self.create_dataframe(ascats, self.ascat_schema)

    def arrange_consequence_df(
        self, consequences: Iterable[models.CNVConsequence] = (models.CNVConsequence(),)
    ) -> sql.DataFrame:
        return self.create_dataframe(consequences, self.consequence_schema)

    def arrange_observation_df(
        self, observations: Iterable[models.CNVObservation] = (models.CNVObservation(),)
    ) -> sql.DataFrame:
        return self.create_dataframe(observations, self.observation_schema)

    @pytest.mark.parametrize("index_name", ("case_centric", "gene_centric"))
    def test__with_observations(self, index_name: str) -> None:
        ascat = models.ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))
        observation_wrapper = models.CNVObservation()
        observation_df = self.arrange_observation_df((observation_wrapper,))

        result_df = df_builders.build_cnv_subtree(
            ascat_df,
            index_name,
            obs_df=observation_df,
        )

        assert result_df.count() == 1
        assert result_df.schema == self.final_other_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.case_id == ascat.case_id
        assert result_row.gene_id == ascat.gene_id
        assert result_row.chromosome == ascat.chromosome
        assert result_row.cnv_change == ascat.cnv_change
        assert result_row.end_position == ascat.end_position
        assert result_row.gene_level_cn == ascat.gene_level_cn
        assert result_row.ncbi_build == ascat.ncbi_build
        assert result_row.start_position == ascat.start_position

        result_observation = more_itertools.one(result_row.observation)
        observation = more_itertools.one(observation_wrapper.observation or ())

        assert result_observation and observation
        assert result_observation.observation_id == observation.observation_id
        assert result_observation.variant_calling and observation.variant_calling
        assert (
            result_observation.variant_calling.variant_caller
            == observation.variant_calling.variant_caller
        )
        assert result_observation.variant_status == observation.variant_status

    def test__with_consequences(self) -> None:
        ascat = models.ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))
        consequence_wrapper = models.CNVConsequence()
        consequence_df = self.arrange_consequence_df((consequence_wrapper,))

        result_df = df_builders.build_cnv_subtree(
            ascat_df,
            "cnv_occurrence_centric",
            cons_df=consequence_df,
            add_fields=("case_id",),
        )

        assert result_df.count() == 1
        assert result_df.schema == self.final_occurrence_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.case_id == ascat.case_id
        assert result_row.chromosome == ascat.chromosome
        assert result_row.cnv_change == ascat.cnv_change
        assert result_row.end_position == ascat.end_position
        assert result_row.gene_level_cn == ascat.gene_level_cn
        assert result_row.ncbi_build == ascat.ncbi_build
        assert result_row.start_position == ascat.start_position
        assert result_row.variant_status == ascat.variant_status

        result_consequence = more_itertools.one(result_row.consequence)
        consequence = more_itertools.one(consequence_wrapper.consequence or ())

        assert result_consequence and consequence
        assert result_consequence.consequence_id == consequence.consequence_id
        assert result_consequence.gene and consequence.gene
        assert result_consequence.gene.biotype == consequence.gene.biotype
        assert result_consequence.gene.gene_id == consequence.gene.gene_id
        assert (
            result_consequence.gene.is_cancer_gene_census
            == consequence.gene.is_cancer_gene_census
        )
        assert result_consequence.gene.symbol == consequence.gene.symbol
