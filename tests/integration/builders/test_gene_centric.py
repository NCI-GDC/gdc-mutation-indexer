import pytest
from pyspark import sql

from mutation_indexer import builders
from tests.integration.utils import join_utils


@pytest.mark.parametrize(
    "path",
    (
        "gene_id",
        "transcripts",
        "transcripts.is_canonical",
        "transcripts.exons",
        "transcripts.domains",
        "case",
        "case.case_id",
    ),
)
def test_gene_centric_path_exists(gene_centric_df: sql.DataFrame, path: str) -> None:
    """
    Chosen paths that have to be present to merge branch
    """
    gene_centric_df.select(path)


def test_cases_per_gene(
    maf_df: sql.DataFrame, cnv_df: sql.DataFrame, gene_centric_df: sql.DataFrame
) -> None:
    # Cases per gene built:
    df = join_utils.unpack_df_list(gene_centric_df, "gene_id", "case", "case_id")
    cpg = join_utils.get_relationship_map(df, ("gene_id", "case_id"))

    # Cases per gene expected:
    df = (
        maf_df.select("case_id", "gene_id").union(cnv_df.select("case_id", "gene_id"))
    ).distinct()
    true_cpg = join_utils.get_relationship_map(df, ("gene_id", "case_id"))

    assert cpg == true_cpg


def test_ssm_per_case(maf_df: sql.DataFrame, gene_centric_df: sql.DataFrame) -> None:
    # SSMs per case built:
    df = join_utils.unpack_df_list(
        gene_centric_df, "gene_id", "case", ("case_id", "ssm")
    )
    df = join_utils.unpack_df_list(df, ("gene_id", "case_id"), "ssm", "ssm_id")

    es_spc = join_utils.get_relationship_map(df, ("gene_id", "case_id", "ssm_id"))

    # SSMs per case expected:
    df = maf_df.select("gene_id", "case_id", "ssm_id")
    spc = join_utils.get_relationship_map(df, ("gene_id", "case_id", "ssm_id"))

    assert es_spc == spc


@pytest.mark.gene_centric_ssm_subtree
def test_ssm_subtree(
    maf_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
    gene_ssm_subtree: sql.DataFrame,
) -> None:
    observation_builder = builders.ObservationBuilder()
    consequence_builder = builders.ConsequenceBuilder()

    # ssm_subtree stats expected:
    cons_df = consequence_builder.build_for_ssm(maf_df, "gene_centric")
    obs_df = observation_builder.build_for_ssm(
        maf_df, primary_aliquot_df, "gene_centric", selector="ssm"
    )

    df = cons_df.join(obs_df, on=["ssm_id"], how="left")

    df = join_utils.unpack_df_list(
        df, ("ssm_id", "observation"), "consequence", "consequence_id"
    )
    df = join_utils.unpack_df_list(
        df, ("ssm_id", "consequence_id"), "observation", "observation_id"
    )
    data = df.collect()
    true_consequence_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id")
    )
    true_observation_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "observation_id")
    )

    # ssm_subtree stats built:
    df = join_utils.unpack_df_list(
        gene_ssm_subtree, (), "ssm", ("ssm_id", "consequence", "observation")
    )
    df = join_utils.unpack_df_list(
        df, ("ssm_id", "observation"), "consequence", "consequence_id"
    )
    df = join_utils.unpack_df_list(
        df, ("ssm_id", "consequence_id"), "observation", "observation_id"
    )
    data = df.collect()
    consequence_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "consequence_id")
    )
    observation_stats = join_utils.get_relationship_map(
        data, ("ssm_id", "observation_id")
    )

    assert consequence_stats == true_consequence_stats
    assert observation_stats == true_observation_stats
