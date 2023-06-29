import random
from typing import Any, Tuple

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
    ("inputs", "expected_uuid"),
    (
        (
            ("ssm", "GRCh38", "chr4", "112382545", "112382545", "SNP", "A", "T"),
            "3439eab1-0c63-50cd-bad7-1ae8ffa8aa01",
        ),
        (
            (
                "ssm_occurrence",
                "642a6e7d-8b15-5f93-9e29-22c9649e9058",
                "13afbde8-e5b5-4f3c-8a9d-daef71560005",
            ),
            "f4222c55-fea2-5b23-a204-482f33492800",
        ),
    ),
)
def test__generate_uuid5__fixed_output_for(
    inputs: Tuple[Any, ...], expected_uuid: str
) -> None:
    """
    Test uuid5 generation
    """
    result_uuid = utils.generate_uuid5(*inputs)

    assert result_uuid == expected_uuid
