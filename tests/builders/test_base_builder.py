import json
from collections import Counter
from pyspark.sql import SQLContext, DataFrameReader, SparkSession
from pyspark.sql.functions import split, explode

import pytest
from normalizer.mapper import ModelMapper

from exports.builders import (
    CaseCentricBuilder,
    GeneCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder,
    CNVCentricBuilder,
    CNVOccurrenceCentricBuilder,
)
from exports.builders.base_builder import get_all_boolean_paths
from tests_config import TestConfig

conf = TestConfig()


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


def test_sample_data_cast_boolean(sqlContext, get_mapping_data):
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.parquet("tests/data/input/sample.parquet")
    dtypes = dict(df.select("gene.is_cancer_gene_census").dtypes)
    assert dtypes["is_cancer_gene_census"] == "array<string>"
    gene_level_cn_df = df.select(explode(df.gene.cnv)).select("col.gene_level_cn")
    dtypes = dict(gene_level_cn_df.dtypes)
    assert dtypes["gene_level_cn"] == "array<string>"
    assert gene_level_cn_df.first().gene_level_cn[0] == "true"
    builder = CaseCentricBuilder(conf, sqlContext)
    new_df = builder.check_and_cast_booleans(df, get_mapping_data)
    dtypes = dict(new_df.select("gene.is_cancer_gene_census").dtypes)
    assert dtypes["is_cancer_gene_census"] == "array<boolean>"
    gene_level_cn_df = new_df.select(explode(new_df.gene.cnv)).select(
        "col.gene_level_cn"
    )
    dtypes = dict(gene_level_cn_df.dtypes)
    assert dtypes["gene_level_cn"] == "array<boolean>"
    assert gene_level_cn_df.first().gene_level_cn[0] is True


@pytest.mark.parametrize(
    "builder_class,index",
    (
        (CaseCentricBuilder, "case_centric"),
        (GeneCentricBuilder, "gene_centric"),
        (SSMCentricBuilder, "ssm_centric"),
        (SSMOccurrenceCentricBuilder, "ssm_occurrence_centric"),
        (CNVCentricBuilder, "cnv_centric"),
        (CNVOccurrenceCentricBuilder, "cnv_occurrence_centric"),
    ),
)
def test_base_builder_cast_boolean(sqlContext, builder_class, index, request):
    builder = builder_class(conf, sqlContext)
    df = request.getfixturevalue("{}_df".format(index))
    index_mapper = ModelMapper(index)
    df = builder.check_and_cast_booleans(df, index_mapper.mapping)
    paths = get_all_boolean_paths(index_mapper.mapping)
    boolean_counts = Counter(path[-1] for path in paths)
    simple_string = df.schema.simpleString()
    print(boolean_counts)
    print(simple_string)
    for field, count in boolean_counts.items():
        s = "{}:boolean".format(field)
        assert simple_string.count(s) == count, "{} boolean not match".format(field)
