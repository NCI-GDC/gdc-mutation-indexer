import json
import pathlib
from collections.abc import Iterable, Sequence

import deepdiff
import pytest
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz.builders import base_builder


@pytest.fixture
def mapping_data(input_dir: pathlib.Path) -> dict:
    with open(input_dir.joinpath("case_centric_mapping.json")) as mapping_fp:
        return json.load(mapping_fp)


def test__get_all_boolean_paths__all_paths_returned(mapping_data: dict) -> None:
    paths = base_builder.get_all_boolean_paths(mapping_data)
    expected_paths = [
        ["gene", "cnv", "gene_level_cn"],
        ["gene", "is_cancer_gene_census"],
    ]
    assert not deepdiff.DeepDiff(
        paths, expected_paths, ignore_order=True, report_repetition=True
    )


def test_sample_data_cast_boolean(
    mapping_data: dict, spark_session: sql.SparkSession, input_dir: pathlib.Path
) -> None:
    df = spark_session.read.parquet(str(input_dir.joinpath("sample.parquet")))
    gene_df = df.select(F.explode("gene")).select("col.*")
    gene_dtypes = dict(gene_df.dtypes)

    cnv_df = gene_df.select(F.explode("cnv")).select("col.*")
    cnv_dtypes = dict(cnv_df.dtypes)

    new_df = base_builder.cast_booleans(df, mapping_data)
    new_gene_df = new_df.select(F.explode("gene")).select("col.*")
    new_gene_dtypes = dict(new_gene_df.dtypes)

    new_cnv_df = new_gene_df.select(F.explode("cnv")).select("col.*")
    new_cnv_dtypes = dict(new_cnv_df.dtypes)

    assert gene_dtypes["is_cancer_gene_census"] == "string"
    assert cnv_dtypes["gene_level_cn"] == "string"
    assert cnv_df.first().gene_level_cn == "true"
    assert new_gene_dtypes["is_cancer_gene_census"] == "boolean"
    assert new_cnv_dtypes["gene_level_cn"] == "boolean"
    assert new_cnv_df.first().gene_level_cn is True


def get_struct(data_type: types.DataType, field: str) -> types.StructType:
    assert isinstance(data_type, (types.StructType, types.ArrayType)), (
        f"Parent field: {field} is of an unexpected type: {type(data_type)}"
    )

    if isinstance(data_type, types.StructType):
        return data_type

    return get_struct(data_type.elementType, field)


def get_field(struct: types.StructType, field: str) -> types.StructType:
    assert field in struct.fieldNames(), (
        f"Cannot resolve {field} in the fields: {struct.fieldNames()}"
    )

    return get_struct(struct[field].dataType, field)


def assert_path_is_boolean(struct: types.StructType, path: Sequence[str]) -> None:
    path_to_field = path[:-1]
    field = path[-1]
    next_struct = struct

    for parent_field in path_to_field:
        next_struct = get_field(next_struct, parent_field)

    assert field in next_struct.fieldNames()
    assert isinstance(next_struct[field].dataType, types.BooleanType)


def assert_all_paths_are_booleans(
    schema: types.StructType, paths: Iterable[Sequence[str]]
) -> None:
    for path in paths:
        assert_path_is_boolean(schema, path)


def test__cast_booleans__case_centric(case_centric_df: sql.DataFrame) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(build.IndexType.CASE_CENTRIC)
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(case_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)


def test__cast_booleans__cnv_centric(cnv_centric_df: sql.DataFrame) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(build.IndexType.CNV_CENTRIC)
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(cnv_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)


def test__cast_booleans__cnv_occurrence_centric(
    cnv_occurrence_centric_df: sql.DataFrame,
) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(
        build.IndexType.CNV_OCCURRENCE_CENTRIC
    )
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(cnv_occurrence_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)


def test__cast_booleans__gene_centric(gene_centric_df: sql.DataFrame) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(build.IndexType.GENE_CENTRIC)
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(gene_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)


def test__cast_booleans__ssm_centric(ssm_centric_df: sql.DataFrame) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(build.IndexType.SSM_CENTRIC)
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(ssm_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)


def test__cast_booleans__ssm_occurrence_centric(
    ssm_occurrence_centric_df: sql.DataFrame,
) -> None:
    index_mapper = es_utils.MappingsLoader().load_mapper(
        build.IndexType.SSM_OCCURRENCE_CENTRIC
    )
    paths = base_builder.get_all_boolean_paths(index_mapper.mappings)

    result_df = base_builder.cast_booleans(ssm_occurrence_centric_df, index_mapper.mappings)
    result_schema = result_df.schema

    assert_all_paths_are_booleans(result_schema, paths)
