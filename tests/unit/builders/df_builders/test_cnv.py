import unittest
from collections.abc import Iterable

import more_itertools
from pyspark import sql

from mutation_indexer.builders import df_builders
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


class TestGetCNVDataFrame(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._ascat_schema = schemas.Viz.Builders.ASCAT.FINAL.load()
        cls._final_schema = schemas.Viz.Builders.DFBuilders.CNV.FINAL.load()

    def arrange_ascat_df(
        self, ascats: Iterable[models.ASCAT] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(ascats, self._ascat_schema)

    def test__single_row(self) -> None:
        ascat = models.ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))

        result_df = df_builders.get_cnv_df(ascat_df, "cnv_centric")

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.chromosome == ascat.chromosome
        assert result_row.cnv_change == ascat.cnv_change
        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.end_position == ascat.end_position
        assert result_row.gene_level_cn == ascat.gene_level_cn
        assert result_row.ncbi_build == ascat.ncbi_build
        assert result_row.start_position == ascat.start_position


class TestBuildCNVSubtree(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._ascat_schema = schemas.Viz.Builders.ASCAT.FINAL.load()
        cls._consequence_schema = schemas.Viz.Builders.Consequence.CNV.FINAL.load()
        cls._observation_schema = (
            schemas.Viz.Builders.Observation.CNV.Other.FINAL.load()
        )
        cls._final_occurrence_schema = (
            schemas.Viz.Builders.DFBuilders.CNV.Occurrence.FINAL.load()
        )
        cls._final_other_schema = schemas.Viz.Builders.DFBuilders.CNV.Other.FINAL.load()

    def arrange_ascat_df(
        self, ascats: Iterable[models.ASCAT] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(ascats, self._ascat_schema)

    def arrange_consequence_df(
        self, consequences: Iterable[models.CNVConsequence] = (models.CNVConsequence(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(consequences, self._consequence_schema)

    def arrange_observation_df(
        self, observations: Iterable[models.CNVObservation] = (models.CNVObservation(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(observations, self._observation_schema)

    @utils.parametrize(("case_centric",), ("gene_centric",))
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
        assert result_df.schema == self._final_other_schema

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
        assert result_df.schema == self._final_occurrence_schema

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
