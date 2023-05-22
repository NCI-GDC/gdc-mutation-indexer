import random
from typing import Any, Optional, Tuple

import more_itertools
import pytest
from pyspark import sql

from exports.builders import utils


def test__percentile__returns_correct_value() -> None:
    """
    Test the percentile util function
    """
    length = (random.randint(0, 50)) * 2 + 1
    v = list(more_itertools.repeatfunc(random.randint, length, 0, 100))
    sorted_v = sorted(v)
    assert utils.percentile(v, 0) == sorted_v[0]
    assert utils.percentile(v, 50) == sorted_v[length // 2]
    assert utils.percentile(v, 100) == sorted_v[-1]


def test__sanitize_aa_change__remove_p_dot(spark_session: sql.SparkSession) -> None:
    input_data = (
        sql.Row(aa_change="a"),
        sql.Row(aa_change="p.b"),
        sql.Row(aa_change="cp."),
    )
    input_df = spark_session.createDataFrame(input_data)

    result_df = utils.sanitize_aa_change(input_df)
    result_data = frozenset(r.aa_change for r in result_df.collect())

    assert result_data == frozenset({"a", "b", "c"})


def test__extract_impact__remove_score(spark_session: sql.SparkSession) -> None:
    input_data = (
        sql.Row(field="possibly_damaging(0.475)"),
        sql.Row(field="deleterious_low_confidence(0)"),
        sql.Row(field="zero_decimal(0.)"),
        sql.Row(field=""),
    )
    input_df = spark_session.createDataFrame(input_data)
    expected_data = frozenset(
        {"possibly_damaging", "deleterious_low_confidence", "zero_decimal", ""}
    )

    result_df = utils.extract_impact(input_df, "field", "field_impact")
    result_data = frozenset(r.field_impact for r in result_df.collect())

    assert result_data == expected_data


def test__extract_score__remove_impact(spark_session: sql.SparkSession) -> None:
    input_data = (
        sql.Row(field="possibly_damaging(0.475)"),
        sql.Row(field="deleterious_low_confidence(0.)"),
        sql.Row(field="zero_decimal(0.1)"),
        sql.Row(field=""),
    )
    input_df = spark_session.createDataFrame(input_data)

    result_df = utils.extract_score(input_df, "field", "field_score")
    result_data = frozenset(r.field_score for r in result_df.collect())

    assert result_data == frozenset({0.475, 0.0, 0.1, None})


def test__sanitize_gene_aa_change__sort_drop_dups_nulls_and_empty(
    spark_session: sql.SparkSession,
) -> None:
    input_data = (sql.Row(gene_aa_change=["c", "a", "a", "", None, "b", "c", "c"]),)
    input_df = spark_session.createDataFrame(input_data)

    result_df = utils.sanitize_gene_aa_change(input_df)
    result_row = more_itertools.one(result_df.collect())

    assert result_row.gene_aa_change == ["a", "b", "c"]


def test__convert_empty_str_to_null_in_col__nulls(
    spark_session: sql.SparkSession,
) -> None:
    input_data = (sql.Row(value="a"), sql.Row(value=""), sql.Row(value="c"))
    input_df = spark_session.createDataFrame(input_data)

    result_df = utils.convert_empty_str_to_null_in_col(input_df, "value")
    result_data = frozenset(r.value for r in result_df.collect())

    assert result_data == frozenset({"a", None, "c"})


def test__extract_aas_position__aa_start_and_end(
    spark_session: sql.SparkSession,
) -> None:
    """
    Test aa_start and aa_end extraction
    """
    input_df = spark_session.createDataFrame((sql.Row(aa_change="p.L1201R"),))

    result_df = utils.extract_aas_position(input_df)
    result_row = more_itertools.one(result_df.collect())

    assert result_row.aa_start == 1201
    assert result_row.aa_end == 1201


@pytest.mark.parametrize(
    (
        "chromosome",
        "variant_type",
        "start_pos",
        "end_pos",
        "ref_allele",
        "tumor_allele",
        "expected_label",
    ),
    (
        ("chr3", "SNP", 41589825, None, "A", "T", "chr3:g.41589825A>T"),
        (
            "chr5",
            "DNP",
            112382500,
            112382501,
            "AC",
            "TG",
            "chr5:g.112382500_112382501delinsTG",
        ),
        (
            "chr5",
            "TNP",
            112382500,
            112382502,
            "ACT",
            "TGA",
            "chr5:g.112382500_112382502delinsTGA",
        ),
        (
            "chr5",
            "ONP",
            112382500,
            112382505,
            "TCGATC",
            "CTAGCT",
            "chr5:g.112382500_112382505delinsCTAGCT",
        ),
        ("chr3", "DEL", 41589825, None, "A", "", "chr3:g.41589825delA"),
        ("chr3", "INS", 41589825, 41589825, "", "T", "chr3:g.41589825_41589825insT"),
        ("chr4", "DEFAULT", 112382545, None, "A", "T", "4"),
    ),
    ids=(
        "snp_variant",
        "dnp_variant",
        "tnp_variant",
        "onp_variant",
        "del_variant",
        "ins_variant",
        "default",
    ),
)
def test__ssm_label__extract(
    chromosome: str,
    variant_type: str,
    start_pos: int,
    end_pos: Optional[int],
    ref_allele: str,
    tumor_allele: str,
    expected_label: str,
) -> None:
    """
    Test ssm label generation
    """
    label = utils.ssm_label(
        chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele
    )

    assert label == expected_label
