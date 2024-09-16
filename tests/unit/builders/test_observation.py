import unittest
from collections.abc import Iterable
from typing import Optional

import more_itertools
from pyspark import sql

from mutation_indexer import builders
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


class TestObservationBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._maf_schema = schemas.Viz.Builders.MAF.FINAL.load()
        cls._ascat_schema = schemas.Viz.Builders.ASCAT.FINAL.load()
        cls._primary_aliquot_schema = schemas.Viz.Builders.PrimaryAliquot.FINAL.load()
        cls._ssm_schemas = {
            "ssm": schemas.Viz.Builders.Observation.SSM.FINAL.load(),
            "other": schemas.Viz.Builders.Observation.Other.FINAL.load(),
        }
        cls._cnv_schemas = {
            "cnv": schemas.Viz.Builders.Observation.CNV.CNV.FINAL.load(),
            "other": schemas.Viz.Builders.Observation.CNV.Other.FINAL.load(),
        }

    def arrange_maf_df(
        self, mafs: Iterable[models.MAF] = (models.MAF(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(mafs, self._maf_schema)

    def arrange_ascat_df(
        self, ascats: Iterable[models.ASCAT] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return utils.create_dataframe(ascats, self._ascat_schema)

    def arrange_primary_aliquot_df(
        self,
        primary_aliquots: Iterable[models.PrimaryAliquot] = (models.PrimaryAliquot(),),
    ) -> sql.DataFrame:
        return utils.create_dataframe(primary_aliquots, self._primary_aliquot_schema)

    def arrange_builder(self) -> builders.ObservationBuilder:
        return builders.ObservationBuilder()

    @utils.parametrize[str, Optional[str], str](
        case_centric=("case_centric", "ssm", "other"),
        gene_centric=("gene_centric", "ssm", "other"),
        ssm_centric=("ssm_centric", None, "ssm"),
        ssm_occurrence_centric=("ssm_occurrence_centric", None, "ssm"),
    )
    def test__build_for_ssm__final_schema(
        self, index_name: str, selector: Optional[str], final_schema: str
    ) -> None:
        maf_df = self.arrange_maf_df()
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, index_name, selector
        )

        assert result_df.count() == 1
        assert result_df.schema == self._ssm_schemas[final_schema]

    def test__build_for_ssm__caller_split(self) -> None:
        maf_df = self.arrange_maf_df((models.MAF(variant_caller="muse;varscan2"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())

        assert len(result_row.observation) == 2
        assert not frozenset(
            observation.variant_calling.variant_caller
            for observation in result_row.observation
        ) ^ frozenset(("muse", "varscan2"))

    def test__build_for_ssm__caller_stripped(self) -> None:
        maf_df = self.arrange_maf_df((models.MAF(variant_caller="***varscan2**"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())
        result_observation = more_itertools.one(result_row.observation)

        assert result_observation.variant_calling.variant_caller == "varscan2"

    def test__build_for_ssm__filter_somatic_snipper(self) -> None:
        maf_df = self.arrange_maf_df((models.MAF(variant_caller="muse;somaticsniper"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())

        assert not any(
            observation.variant_calling.variant_caller == "somaticsniper"
            for observation in result_row.observation
        )

    @utils.parametrize[str, Optional[str], str](
        case_centric=("case_centric", "cnv", "other"),
        cnv_centric=("cnv_centric", None, "cnv"),
        cnv_occurrence_centric=("cnv_occurrence_centric", None, "cnv"),
        gene_centric=("gene_centric", "cnv", "other"),
    )
    def test__build_for_cnv__final_schema(
        self, index: str, selector: Optional[str], final_schema: str
    ) -> None:
        ascat_df = self.arrange_ascat_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, index, selector)

        assert result_df.count() == 1
        assert result_df.schema == self._cnv_schemas[final_schema]

    def test__build_for_cnv__data_translated(self) -> None:
        ascat = models.ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_row = more_itertools.one(result_df.collect())
        result_observation = more_itertools.one(result_row.observation)

        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.case_id == ascat.case_id
        assert result_row.occurrence_id == ascat.occurrence_id
        assert result_observation.observation_id == ascat.observation_id
        assert result_observation.variant_calling.variant_caller == ascat.variant_caller
        assert result_observation.variant_status == ascat.variant_status

    def test__build_for_cnv__observation_grouped_by_cnv_case_occurrence_ids(
        self,
    ) -> None:
        base_group = ("cnv-0", "case-0", "occ-0")
        cnv_group = ("cnv-1", "case-0", "occ-0")
        case_group = ("cnv-0", "case-1", "occ-0")
        occ_group = ("cnv-0", "case-0", "occ-1")
        ascats = (
            models.ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
            models.ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-1",
            ),
            models.ASCAT(cnv_id="cnv-1", case_id="case-0", occurrence_id="occ-0"),
            models.ASCAT(cnv_id="cnv-0", case_id="case-1", occurrence_id="occ-0"),
            models.ASCAT(cnv_id="cnv-0", case_id="case-0", occurrence_id="occ-1"),
        )
        ascat_df = self.arrange_ascat_df(ascats)
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_rows = {
            (r.cnv_id, r.case_id, r.occurrence_id): r for r in result_df.collect()
        }

        assert len(result_rows) == 4
        assert base_group in result_rows
        assert cnv_group in result_rows
        assert case_group in result_rows
        assert occ_group in result_rows
        assert len(result_rows[base_group].observation) == 2
        assert len(result_rows[cnv_group].observation) == 1
        assert len(result_rows[case_group].observation) == 1
        assert len(result_rows[occ_group].observation) == 1

    def test__build_for_cnv__observation_aggregates_no_duplicates(self) -> None:
        ascats = (
            models.ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
            models.ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
        )
        ascat_df = self.arrange_ascat_df(ascats)
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_rows = result_df.collect()

        assert len(result_rows) == 1
