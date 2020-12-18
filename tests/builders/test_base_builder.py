import json
from collections import Counter

import pytest
from normalizer.mapper import ModelMapper
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

from exports.builders.base_builder import get_all_boolean_paths, cast_booleans



@pytest.fixture
def get_mapping_data():
    with open("tests/data/input/case_centric_mapping.json") as mapping_fp:
        data = json.load(mapping_fp)
    return data


def test_get_all_boolean_paths(get_mapping_data):
    paths = get_all_boolean_paths(get_mapping_data)
    assert paths == [
        [u"gene", u"cnv", u"gene_level_cn"],
        [u"gene", u"is_cancer_gene_census"],
    ]


def test_sample_data_cast_boolean(get_mapping_data):
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.parquet("tests/data/input/sample.parquet")
    gene_df = df.select(explode("gene")).select("col.*")
    gene_dtypes = dict(gene_df.dtypes)
    assert gene_dtypes["is_cancer_gene_census"] == "string"
    cnv_df = gene_df.select(explode("cnv")).select("col.*")
    cnv_dtypes = dict(cnv_df.dtypes)
    assert cnv_dtypes["gene_level_cn"] == "string"
    assert cnv_df.first().gene_level_cn == "true"
    new_df = cast_booleans(df, get_mapping_data)
    new_gene_df = new_df.select(explode("gene")).select("col.*")
    new_gene_dtypes = dict(new_gene_df.dtypes)
    assert new_gene_dtypes["is_cancer_gene_census"] == "boolean"
    new_cnv_df = new_gene_df.select(explode("cnv")).select("col.*")
    new_cnv_dtypes = dict(new_cnv_df.dtypes)
    assert new_cnv_dtypes["gene_level_cn"] == "boolean"
    assert new_cnv_df.first().gene_level_cn is True


@pytest.mark.parametrize(
    "index",
    (
        "case_centric",
        "gene_centric",
        "ssm_centric",
        "ssm_occurrence_centric",
        "cnv_centric",
        "cnv_occurrence_centric",
    ),
)
def test_base_builder_cast_boolean(sqlContext, index, request):
    df = request.getfixturevalue("{}_df".format(index))
    index_mapper = ModelMapper(index)
    df = cast_booleans(df, index_mapper.mapping)
    paths = get_all_boolean_paths(index_mapper.mapping)
    boolean_counts = Counter(path[-1] for path in paths)
    simple_string = df.schema.simpleString()
    for field, count in boolean_counts.items():
        s = "{}:boolean".format(field)
        assert simple_string.count(s) == count, "{} boolean not match".format(field)
