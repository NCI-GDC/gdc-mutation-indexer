import json

import deepdiff
import pytest
from pyspark import sql
from pyspark.sql import functions as pyspark_functions

from mutation_indexer.viz import builders


class TestObservationBuilder:
    """Test intermediate result from the observation builder"""

    @pytest.fixture(scope="class")
    def builder(self):
        return builders.ObservationBuilder()

    @pytest.mark.parametrize("index_name", ("ssm_centric", "ssm_occurrence_centric"))
    def test__build_for_ssm__join_columns(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert set(result_df.columns) == {
            "case_id",
            "ssm_id",
            "observation",
            "occurrence_id",
        }

    @pytest.mark.parametrize("index_name", ("cnv_centric", "cnv_occurrence_centric"))
    def test__build_for_cnv__join_columns(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert set(result_df.columns) == {
            "case_id",
            "cnv_id",
            "observation",
            "occurrence_id",
        }

    @pytest.mark.parametrize("index_name", ("ssm_centric", "ssm_occurrence_centric"))
    def test__build_for_ssm__observation_id(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert "observation_id" in (
            result_df.select(pyspark_functions.explode("observation").alias("observation"))
            .select("observation.*")
            .columns
        )

    @pytest.mark.parametrize("index_name", ("cnv_centric", "cnv_occurrence_centric"))
    def test__build_for_cnv__observation_id(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert "observation_id" in (
            result_df.select(pyspark_functions.explode("observation").alias("observation"))
            .select("observation.*")
            .columns
        )

    @pytest.mark.parametrize("index_name", ("ssm_centric", "ssm_occurrence_centric"))
    def test__build_for_ssm__observation_count(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        observation_count = maf_df.select("case_id", "ssm_id").distinct().count()

        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert result_df.count() == observation_count

    @pytest.mark.parametrize("index_name", ("cnv_centric", "cnv_occurrence_centric"))
    def test__build_for_cnv__observation_count(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        observation_count = cnv_df.select("case_id", "cnv_id").distinct().count()

        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert result_df.count() == observation_count

    @pytest.mark.parametrize("index_name", ("ssm_centric", "ssm_occurrence_centric"))
    def test__build_for_ssm__observation_values(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        expected_opservations = map(
            json.loads,
            (maf_df.select("case_id", "ssm_id").distinct().toJSON().collect()),
        )

        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        actual_observations = map(
            json.loads, (result_df.select("case_id", "ssm_id").toJSON().collect())
        )
        assert not deepdiff.DeepDiff(
            expected_opservations, actual_observations, ignore_order=True
        )

    @pytest.mark.parametrize("index_name", ("cnv_centric", "cnv_occurrence_centric"))
    def test__build_for_cnv__observation_values(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        expected_opservations = map(
            json.loads,
            (cnv_df.select("case_id", "cnv_id").distinct().toJSON().collect()),
        )

        result_df = builder.build_for_cnv(cnv_df, index_name)

        actual_observations = map(
            json.loads, (result_df.select("case_id", "cnv_id").toJSON().collect())
        )
        assert not deepdiff.DeepDiff(
            expected_opservations, actual_observations, ignore_order=True
        )

    @pytest.mark.parametrize("index_name", ("ssm_centric", "ssm_occurrence_centric"))
    def test__build_for_ssm__variant_caller(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        exploded_variant_caller_counts: int,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        actual_counts = dict(
            result_df.select(pyspark_functions.explode("observation").alias("observation"))
            .groupBy("observation.variant_calling.variant_caller")
            .count()
            .collect()
        )

        assert actual_counts == exploded_variant_caller_counts
