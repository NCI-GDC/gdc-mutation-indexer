import json
from collections import Counter

import pytest
from normalizer.mapper import ModelMapper

from exports.builders import CaseCentricBuilder, GeneCentricBuilder, SSMCentricBuilder, SSMOccurrenceCentricBuilder, \
    CNVCentricBuilder, CNVOccurrenceCentricBuilder
from exports.builders.base_builder import get_all_boolean_paths
from tests_config import TestConfig

conf = TestConfig()


def test_get_all_boolean_paths():
    with open('tests/data/input/mapping.json') as mapping_fp:
        data = json.load(mapping_fp)
        paths = get_all_boolean_paths(data)
        assert paths == [[u'gene', u'cnv', u'gene_level_cn'], [u'gene', u'is_cancer_gene_census']]


@pytest.mark.parametrize("builder_class,index", ((CaseCentricBuilder, "case_centric"), (GeneCentricBuilder, "gene_centric"), (SSMCentricBuilder, "ssm_centric"), (SSMOccurrenceCentricBuilder, "ssm_occurrence_centric"), (CNVCentricBuilder, "cnv_centric"), (CNVOccurrenceCentricBuilder, "cnv_occurrence_centric")))
def test_base_builder(sqlContext, maf_df, gistic_df, case_df, builder_class, index):
    builder = builder_class(conf, sqlContext)
    if index == "case_centric":
        builder.build(maf_df, gistic_df, case_df)
    elif index == "gene_centric":
        builder.build(maf_df, gistic_df, case_df.drop("summary"))
    elif index == "ssm_centric" or index == "ssm_occurrence_centric":
        builder.build(maf_df, case_df.drop("summary"))
    else:
        builder.build(gistic_df, case_df.drop("summary"))
    df = builder.load()
    index_mapper = ModelMapper(index)
    paths = get_all_boolean_paths(index_mapper.mapping)
    boolean_counts = Counter(path[-1] for path in paths)
    simple_string = df.schema.simpleString()
    print(boolean_counts)
    print(simple_string)
    for field, count in boolean_counts.items():
        s = "{}:boolean".format(field)
        assert simple_string.count(s) == count, "{} boolean not match".format(field)
