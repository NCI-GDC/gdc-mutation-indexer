import os
import pytest

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import CaseCentricBuilder
from utils.json_metrics import CaseCentricStats
from utils.json_test_utils import (
                                   validate_two_nested_jsons_joining,
                                   validate_two_nested_jsons,
                                   __gathering_statistic_info,
                                   KEY_VALUE_SEPARATOR,
                                   DiffsReporter,
                                  )

builder = CaseCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def case_centric_index(sqlContext, maf_df):
    """ Generates a case centric index for testing """
    es = T.generate_index(sqlContext, maf_df)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def case_stats(case_centric_index):
    docs = case_centric_index.search(index=conf.indices['case_centric'],
                                     doc_type='case_centric',
                                     body={"query": {"match_all": {}}},
                                     size=1000)
    yield CaseCentricStats(docs['hits']['hits'])


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
def test_case_centric_summary_stats(case_stats, maf_stats, stat):
    case_stat = getattr(case_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    if conf.indices_are_pruned:
        if stat in ['Ngenes', 'NUniqMut', 'Nconseq']:
            return
    assert case_stat == maf_stat


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_join(case_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)
    diffs = validate_two_nested_jsons_joining("case{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                              es_doc, true_doc)
    assert diffs == []


@pytest.mark.skipif(conf.skip_in_depth_tests,
                    reason="Only test after having correct data")
@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_in_depth(case_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)

    # Top level keys check
    assert set(es_doc.keys()) == set(true_doc.keys())

    # Join cardinality check
    diffs = validate_two_nested_jsons("case{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc, true_doc)

    if diffs:
        diffs.append(__gathering_statistic_info(diffs))

    DiffsReporter.report_diffs(diffs, builder.index_name)

    assert diffs == []
