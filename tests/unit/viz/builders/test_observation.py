from typing import Optional, Tuple

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.FINAL.load()


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.Viz.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def ssm_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.SSM.FINAL.load()


@pytest.fixture(scope="class")
def other_ssm_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.Other.FINAL.load()


@pytest.fixture(scope="class")
def cnv_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.CNV.CNV.FINAL.load()


@pytest.fixture(scope="class")
def cnv_other_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.CNV.Other.FINAL.load()


class TestObservationBuilder:
    @pytest.fixture(autouse=True)
    def fixture_set_up(
        self,
        spark_session: sql.SparkSession,
        maf_schema: types.StructType,
        ascat_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        ssm_observation_schema: types.StructType,
        other_ssm_observation_schema: types.StructType,
        cnv_observation_schema: types.StructType,
        cnv_other_observation_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.maf_schema = maf_schema
        self.ascat_schema = ascat_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.ssm_schemas = {
            "ssm": ssm_observation_schema,
            "other": other_ssm_observation_schema,
        }
        self.cnv_schemas = {
            "cnv": cnv_observation_schema,
            "other": cnv_other_observation_schema,
        }

    def arrange_maf_df(
        self, mafs: Tuple[models.MAF, ...] = (models.MAF(),)
    ) -> sql.DataFrame:
        return self.spark_session.createDataFrame(
            mafs,  # type: ignore
            schema=self.maf_schema,
        )

    def arrange_ascat_df(
        self, ascats: Tuple[models.ASCAT, ...] = (models.ASCAT(),)
    ) -> sql.DataFrame:
        return self.spark_session.createDataFrame(
            ascats,  # type: ignore
            schema=self.ascat_schema,
        )

    def arrange_primary_aliquot_df(
        self,
        primary_aliquots: Tuple[models.PrimaryAliquot, ...] = (
            models.PrimaryAliquot(),
        ),
    ) -> sql.DataFrame:
        return self.spark_session.createDataFrame(
            primary_aliquots,  # type: ignore
            schema=self.primary_aliquot_schema,
        )

    def arrange_builder(self) -> builders.ObservationBuilder:
        return builders.ObservationBuilder()

    @pytest.mark.parametrize(
        ("index_name", "selector", "final_schema"),
        (
            pytest.param("case_centric", "ssm", "other", id="case_centric"),
            pytest.param("gene_centric", "ssm", "other", id="gene_centric"),
            pytest.param("ssm_centric", None, "ssm", id="ssm_centric"),
            pytest.param(
                "ssm_occurrence_centric", None, "ssm", id="ssm_occurrence_centric"
            ),
        ),
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
        assert result_df.schema == self.ssm_schemas[final_schema]

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
        result_df.show()
        result_row = more_itertools.one(result_df.collect())

        assert not any(
            observation.variant_calling.variant_caller == "somaticsniper"
            for observation in result_row.observation
        )

    @pytest.mark.parametrize(
        ("index", "selector", "final_schema"),
        (
            pytest.param("case_centric", "cnv", "other", id="case_centric"),
            pytest.param("cnv_centric", None, "cnv", id="cnv_centric"),
            pytest.param(
                "cnv_occurrence_centric", None, "cnv", id="cnv_occurrence_centric"
            ),
            pytest.param("gene_centric", "cnv", "other", id="gene_centric"),
        ),
    )
    def test__build_for_cnv__final_schema(
        self, index: str, selector: Optional[str], final_schema: str
    ) -> None:
        ascat_df = self.arrange_ascat_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, index, selector)

        assert result_df.count() == 1
        assert result_df.schema == self.cnv_schemas[final_schema]

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
