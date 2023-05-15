from collections.abc import Iterable, Set
from typing import Any

import pytest
from pyspark import sql
from pyspark.sql import functions as F

from exports.builders import df_builders, utils


def is_sub(subset: dict, superset: Iterable[Any], mapping: dict) -> bool:
    result = True
    for item in subset.items():
        (key, val) = item
        if type(val) is dict:
            result = result and is_sub(val, superset, mapping)
        elif item not in superset:
            if key in mapping and val is not None:
                if (mapping[key].get("default"), val) in superset:
                    continue
                result = False

    return result


def assert_from_df(
    df: sql.DataFrame, row: sql.Row, join_by: str, mapping: dict
) -> None:
    item = row.asDict(recursive=True)
    print(item.keys())
    filtered_dict = {}
    filtered_list = df.filter(F.col(join_by) == item[join_by]).collect()
    for it in filtered_list:
        filtered_dict.update(it.asDict(recursive=True))
    assert is_sub(item, filtered_dict.items(), mapping)


def get_must_have_keys(keys: Iterable[str]) -> Set[str]:
    stopwords = ("copy_to", "_autocomplete", "gene_aa_change")

    return frozenset(k for k in keys if all(s not in k for s in stopwords))


## ANNOTATIONS
@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_annotation_df__simple_df(maf_df: sql.DataFrame, index_type: str) -> None:
    # the input_df has entries with duplicated id but different values
    input_df = maf_df.drop_duplicates(subset=["transcript_id"])

    df = df_builders.get_annotation_df(input_df, index_type)
    mapping = utils.select_mapping(index_type, "annotation")["properties"]

    # Do not check for unwanted keys
    must_have_keys = get_must_have_keys(mapping.keys())

    # make sure all required keys exist
    assert frozenset(df.columns) == must_have_keys

    # make sure that values came from input_df
    assert_from_df(input_df, df.first(), "transcript_id", mapping=mapping)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_annotation_df__drop_fields(
    maf_df: sql.DataFrame, index_type: str
) -> None:
    fields_to_delete = ("ssm_id", "mutation_subtype")

    df = df_builders.get_annotation_df(maf_df, index_type, drop_fields=fields_to_delete)

    assert all(f not in df.columns for f in fields_to_delete)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_annotation_df__unique_fields(
    maf_df: sql.DataFrame, index_type: str
) -> None:
    unique_fields = ("vep_impact",)
    df = df_builders.get_annotation_df(maf_df, index_type, unique_fields=None)
    df_unique = df_builders.get_annotation_df(
        maf_df, index_type, unique_fields=unique_fields
    )

    assert df_unique.count() == (df.select(*unique_fields).distinct().count())


def test__get_annotation_df__add_fields(
    maf_df: sql.DataFrame,
) -> None:
    df = df_builders.get_annotation_df(maf_df, "case_centric", add_fields=["case_id"])

    assert "case_id" in df.columns


## GENE
@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_gene_df__ssm_simple_df(maf_df: sql.DataFrame, index_type: str) -> None:
    # the input_df has entries with duplicated id but different values
    input_df = maf_df.drop_duplicates(subset=["gene_id"])

    df = df_builders.get_gene_df(input_df, index_type)
    mapping = utils.select_mapping(index_type, "gene")["properties"]

    # Do not check for unwanted keys
    must_have_keys = get_must_have_keys(mapping.keys())

    # make sure all required keys exist
    assert frozenset(df.columns) == must_have_keys

    # make sure that values came from input_df
    assert_from_df(input_df, df.first(), "gene_id", mapping=mapping)


@pytest.mark.parametrize(
    "index_type",
    ("cnv_centric", "cnv_occurrence_centric"),
)
def test__get_gene_df__cnv_simple_df(cnv_df: sql.DataFrame, index_type: str) -> None:
    # the input_df has entries with duplicated id but different values
    input_df = cnv_df.drop_duplicates(subset=["gene_id"])

    df = df_builders.get_gene_df(input_df, index_type)
    mapping = utils.select_mapping(index_type, "gene")["properties"]

    # Do not check for unwanted keys
    must_have_keys = get_must_have_keys(mapping.keys())

    # make sure all required keys exist
    assert frozenset(df.columns) == must_have_keys

    # make sure that values came from input_df
    assert_from_df(input_df, df.first(), "gene_id", mapping=mapping)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_gene_df__ssm_drop_fields(maf_df: sql.DataFrame, index_type: str) -> None:
    fields_to_delete = ("ssm_id", "mutation_subtype")

    df = df_builders.get_gene_df(maf_df, index_type, drop_fields=fields_to_delete)

    assert all(f not in df.columns for f in fields_to_delete)


@pytest.mark.parametrize(
    "index_type",
    ("cnv_centric", "cnv_occurrence_centric"),
)
def test__get_gene_df__cnv_drop_fields(cnv_df: sql.DataFrame, index_type: str) -> None:
    fields_to_delete = ("cnv_id", "cnv_change")

    df = df_builders.get_gene_df(cnv_df, index_type, drop_fields=fields_to_delete)

    assert all(f not in df.columns for f in fields_to_delete)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_gene_df__ssm_unique_fields(
    maf_df: sql.DataFrame, index_type: str
) -> None:
    unique_fields = ("biotype",)
    df = df_builders.get_gene_df(maf_df, index_type, unique_fields=None)
    df_unique = df_builders.get_gene_df(maf_df, index_type, unique_fields=unique_fields)

    assert df_unique.count() == (df.select(*unique_fields).distinct().count())


@pytest.mark.parametrize(
    "index_type",
    ("cnv_centric", "cnv_occurrence_centric"),
)
def test__get_gene_df__cnv_unique_fields(
    cnv_df: sql.DataFrame, index_type: str
) -> None:
    unique_fields = ("biotype",)
    df = df_builders.get_gene_df(cnv_df, index_type, unique_fields=None)
    df_unique = df_builders.get_gene_df(cnv_df, index_type, unique_fields=unique_fields)

    assert df_unique.count() == (df.select(*unique_fields).distinct().count())


def test__get_gene_df__add_fields(
    maf_df: sql.DataFrame,
) -> None:
    df = df_builders.get_gene_df(maf_df, "case_centric", add_fields=["case_id"])

    assert "case_id" in df.columns


## TRANSCRIPT
@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_transcript_df__simple_df(maf_df: sql.DataFrame, index_type: str) -> None:
    # the input_df has entries with duplicated id but different values
    input_df = maf_df.drop_duplicates(subset=["transcript_id"])

    df = df_builders.get_transcript_df(input_df, index_type)
    mapping = utils.select_mapping(index_type, "transcript")["properties"]

    # Do not check for unwanted keys
    must_have_keys = get_must_have_keys(mapping.keys())

    # make sure all required keys exist
    assert frozenset(df.columns) == must_have_keys

    # make sure that values came from input_df
    assert_from_df(input_df, df.first(), "transcript_id", mapping=mapping)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_transcript_df__drop_fields(
    maf_df: sql.DataFrame, index_type: str
) -> None:
    fields_to_delete = ("ssm_id", "mutation_subtype")

    df = df_builders.get_transcript_df(maf_df, index_type, drop_fields=fields_to_delete)

    assert all(f not in df.columns for f in fields_to_delete)


@pytest.mark.parametrize(
    "index_type",
    ("case_centric", "gene_centric", "ssm_centric", "ssm_occurrence_centric"),
)
def test__get_transcript_df__unique_fields(
    maf_df: sql.DataFrame, index_type: str
) -> None:
    unique_fields = ("consequence_type",)
    df = df_builders.get_transcript_df(maf_df, index_type, unique_fields=None)
    df_unique = df_builders.get_transcript_df(
        maf_df, index_type, unique_fields=unique_fields
    )

    assert df_unique.count() == (df.select(*unique_fields).distinct().count())


def test__get_transcript_df__add_fields(
    maf_df: sql.DataFrame,
) -> None:
    df = df_builders.get_transcript_df(maf_df, "case_centric", add_fields=["case_id"])

    assert "case_id" in df.columns
