import json
from collections import Counter

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


def test_get_all_boolean_paths():
    with open("tests/data/input/mapping.json") as mapping_fp:
        data = json.load(mapping_fp)
        paths = get_all_boolean_paths(data)
        assert sorted(paths) == [
            [u"gene", u"cnv", u"gene_level_cn"],
            [u"gene", u"is_cancer_gene_census"],
        ]


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
