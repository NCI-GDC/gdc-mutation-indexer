import pytest
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders
from mutation_indexer.builders import utils
from tests.integration.utils import join_utils


@pytest.mark.parametrize(
    "path",
    (
        "case",
        "case.available_variation_data",
        "case.observation",
        "ssm",
        "ssm.consequence",
        "ssm.consequence.transcript",
        "ssm.consequence.transcript.gene",
        "ssm.consequence.transcript.gene.symbol",
        "ssm.consequence.transcript.gene.biotype",
        "ssm.consequence.transcript.annotation",
    ),
)
def test_ssm_occurrence_centric_path_exists(
    ssm_occurrence_centric_df: sql.DataFrame, path: str
) -> None:
    """
    Chosen paths that have to be present to merge branch
    """
    ssm_occurrence_centric_df.select(path)


def test_consequences_per_ssm_occurrence(
    ssm_transcript_df: sql.DataFrame, ssm_occurrence_centric_df: sql.DataFrame
) -> None:
    # Consequences per SSM Occurrence built:
    df = ssm_occurrence_centric_df.select("ssm_occurrence_id", "ssm.ssm_id")
    ssm_occ_to_ssm = join_utils.get_relationship_map(
        df, ("ssm_occurrence_id", "ssm_id")
    )

    # SSM to SSM Occurrence expected:
    df = (
        ssm_transcript_df.withColumn(
            "consequence_id",
            utils.uuid5_col(
                F.lit("ssm_consequence"), F.col("ssm_id"), F.col("transcript_id")
            ),
        )
        .withColumn("ssm_occurrence_id", F.col("occurrence_id"))
        .select(
            "ssm_occurrence_id", "ssm_id", "consequence_id", "transcript_id", "gene_id"
        )
    )

    true_ssm_occ_to_ssm = join_utils.get_relationship_map(
        df, ("ssm_occurrence_id", "ssm_id")
    )

    assert ssm_occ_to_ssm == true_ssm_occ_to_ssm


def test_observations_per_ssm_occurrence(
    maf_df: sql.DataFrame, ssm_occurrence_centric_df: sql.DataFrame
) -> None:
    # The observation ID is calculated when the observation dataframe is built,
    # and is not included in the MAF dataframe, so we can't compute the "true"
    # occurrence ID -> observation ID mapping based on the MAF dataframe alone.
    # Use the tumor sample barcode as an approximation of observation ID.

    # Tumor samples and Cases per SSM Occurrence built:
    df = join_utils.unpack_df_list(
        ssm_occurrence_centric_df,
        ("ssm_occurrence_id", "case.case_id"),
        "case.observation",
        "sample.tumor_sample_barcode",
    )

    spo = join_utils.get_relationship_map(
        df, ("ssm_occurrence_id", "tumor_sample_barcode")
    )
    cpo = join_utils.get_relationship_map(df, ("ssm_occurrence_id", "case_id"))

    # Observations and Cases per SSM Occurrence expected:
    df = maf_df.withColumn("ssm_occurrence_id", F.col("occurrence_id")).select(
        "ssm_occurrence_id", "case_id", "tumor_sample_barcode"
    )

    true_spo = join_utils.get_relationship_map(
        df, ("ssm_occurrence_id", "tumor_sample_barcode")
    )
    true_cpo = join_utils.get_relationship_map(df, ("ssm_occurrence_id", "case_id"))

    assert spo == true_spo
    assert cpo == true_cpo


@pytest.mark.ssm_occurrence_centric_ssm_subtree
def test_ssm_subtree(
    maf_df: sql.DataFrame, ssm_occurrence_ssm_subtree: sql.DataFrame
) -> None:
    fields_to_unpack = (
        "consequence_id",
        "transcript.transcript_id",
        "transcript.gene.gene_id",
    )

    # ssm_subtree stats expected:
    cons_df = builders.ConsequenceBuilder().build_for_ssm(
        maf_df, "ssm_occurrence_centric", join_gene=True
    )
    df = join_utils.unpack_df_list(cons_df, "ssm_id", "consequence", fields_to_unpack)
    data = df.collect()
    true_transcript_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id", "transcript_id")
    )
    true_gene_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id", "gene_id")
    )

    # ssm_subtree stats built:
    df = join_utils.unpack_df_list(
        ssm_occurrence_ssm_subtree, "ssm_id", "ssm.consequence", fields_to_unpack
    )
    data = df.collect()
    transcript_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id", "transcript_id")
    )
    gene_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id", "gene_id")
    )

    assert transcript_stats == true_transcript_stats
    assert gene_stats == true_gene_stats
