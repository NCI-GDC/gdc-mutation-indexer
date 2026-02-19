import pytest
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.viz import builders
from tests.integration.utils import join_utils


def test_consequences_per_cnv_occurrence(
    cnv_df: sql.DataFrame, cnv_occurrence_centric_df: sql.DataFrame
) -> None:
    # Consequences per CNV Occurrence built:
    df = cnv_occurrence_centric_df.select("cnv_occurrence_id", "cnv.cnv_id")
    cnv_occ_to_cnv = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "cnv_id"))

    # CNV to CNV Occurrence expected:
    df = cnv_df.withColumnRenamed("occurrence_id", "cnv_occurrence_id")
    true_cnv_occ_to_cnv = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "cnv_id"))

    assert cnv_occ_to_cnv == true_cnv_occ_to_cnv


def test_observations_per_cnv_occurrence(
    cnv_df: sql.DataFrame, cnv_occurrence_centric_df: sql.DataFrame
) -> None:
    # Observations and Cases per CNV Occurrence built:
    df = join_utils.unpack_df_list(
        cnv_occurrence_centric_df,
        ["cnv_occurrence_id", "case.case_id"],
        "case.observation",
        "observation_id",
    )

    opo = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "observation_id"))
    cpo = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "case_id"))

    # Observations and Cases per CNV Occurrence expected:
    df = cnv_df.withColumn("cnv_occurrence_id", F.col("occurrence_id")).select(
        "cnv_occurrence_id", "observation_id", "case_id"
    )
    true_opo = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "observation_id"))
    true_cpo = join_utils.get_relationship_map(df, ("cnv_occurrence_id", "case_id"))

    assert opo == true_opo
    assert cpo == true_cpo


@pytest.mark.cnv_occurrence_centric_cnv_subtree
def test_cnv_subtree(
    cnv_df: sql.DataFrame,
    cnv_occurrence_centric_df: sql.DataFrame,
) -> None:
    fields_to_unpack = ("consequence_id", "gene.gene_id")

    # ssm_subtree stats expected:
    cons_df = builders.ConsequenceBuilder().build_for_cnv(cnv_df, "cnv_occurrence_centric")
    df = join_utils.unpack_df_list(cons_df, "cnv_id", "consequence", fields_to_unpack)
    data = df.collect()
    true_stats = join_utils.get_relationship_map(data, ("cnv_id", "consequence_id", "gene_id"))

    # ssm_subtree stats built:
    df = join_utils.unpack_df_list(
        cnv_occurrence_centric_df, "cnv.cnv_id", "cnv.consequence", fields_to_unpack
    )
    data = df.collect()
    stats = join_utils.get_relationship_map(data, ("cnv_id", "consequence_id", "gene_id"))

    assert stats == true_stats
