from typing import AbstractSet

from pyspark import sql

from tests.integration.utils import join_utils


def test_genes_per_case(
    maf_df: sql.DataFrame, cnv_df: sql.DataFrame, case_centric_df: sql.DataFrame
) -> None:
    # Genes per case built:
    df = join_utils.unpack_df_list(case_centric_df, "case_id", "gene", "gene_id")
    gpc = join_utils.get_relationship_map(df, ("case_id", "gene_id"))

    # Genes per case expected:
    df = (
        maf_df.select("case_id", "gene_id").union(cnv_df.select("case_id", "gene_id"))
    ).distinct()
    true_gpc = join_utils.get_relationship_map(df, ("case_id", "gene_id"))

    assert gpc == true_gpc


def test_ssm_per_gene(maf_df: sql.DataFrame, case_centric_df: sql.DataFrame) -> None:
    # SSMs per gene built:
    df = join_utils.unpack_df_list(
        case_centric_df, "case_id", "gene", ("gene_id", "ssm")
    )
    df = join_utils.unpack_df_list(df, ("case_id", "gene_id"), "ssm", "ssm_id")
    es_spg = join_utils.get_relationship_map(df, ("case_id", "gene_id", "ssm_id"))

    # SSMs per gene expected:
    df = maf_df.select("gene_id", "case_id", "ssm_id")
    spg = join_utils.get_relationship_map(df, ("case_id", "gene_id", "ssm_id"))

    assert es_spg == spg


def test_case_centric_counts(
    case_centric_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
    all_cases: AbstractSet[str],
    all_maf_cases: AbstractSet[str],
) -> None:
    """
    Test "empty cases"

    Confirm that we index cases even if they have no maf or cnv data.
    This is necessary for the portal to visualize such cases.
    """
    cases_built = frozenset(
        c.case_id
        for c in case_centric_df.select("case_id").distinct().toLocalIterator()
    )
    all_cnv_cases = frozenset(
        r.case_id for r in cnv_df.select("case_id").distinct().toLocalIterator()
    )
    cases_with_data = all_maf_cases | all_cnv_cases
    empty_cases = all_cases - cases_with_data

    # confirm that we have at least one empty case in our test data
    assert empty_cases

    # check that all cases were built (even empty ones)
    assert all_cases == cases_built


def test_available_variation_data(
    case_centric_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
    all_cases: AbstractSet[str],
    all_maf_cases: AbstractSet[str],
) -> None:
    """
    Test that available_variation_data is correctly populated:
        * ['cnv'] - for cnv-only cases
        * ['ssm'] - for cases in the maf header that do not have cnv data
        * ['cnv', 'ssm'] - for cases that have both maf and cnv data
        * [] - for cases that have no maf or cnv data
    """

    assert "available_variation_data" in case_centric_df.columns

    gistic_cases = frozenset(
        r.case_id for r in cnv_df.select("case_id").distinct().toLocalIterator()
    )
    maf_cases = all_maf_cases  # includes cases in maf header without ssms

    common_cases = gistic_cases & maf_cases
    cnv_cases = gistic_cases - maf_cases
    ssm_cases = maf_cases - gistic_cases
    empty_cases = all_cases - gistic_cases - maf_cases

    assert common_cases, "there were no common cases found in test data"
    assert cnv_cases, "there were no cnv cases found in test data"
    assert ssm_cases, "there were no ssm cases found in test data"
    assert empty_cases, "there were no empty cases found in test data"

    # Check that 'available_variation_data' is populated correctly
    for row in case_centric_df.select(
        "case_id", "available_variation_data"
    ).toLocalIterator():
        if row.case_id in common_cases:
            assert row.available_variation_data == ["cnv", "ssm"]
        elif row.case_id in cnv_cases:
            assert row.available_variation_data == ["cnv"]
        elif row.case_id in ssm_cases:
            assert row.available_variation_data == ["ssm"]
        elif row.case_id in empty_cases:
            assert row.available_variation_data == []
