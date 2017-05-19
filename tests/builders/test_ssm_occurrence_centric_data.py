import os
import pytest

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMOccurrenceCentricBuilder
from utils.json_metrics import SSMOcurrenceCentricStats
from utils.json_test_utils import (
                                   validate_two_nested_jsons,
                                   validate_two_nested_jsons_joining,
                                   __gathering_statistic_info,
                                   KEY_VALUE_SEPARATOR,
                                   DiffsReporter,
                                   )

builder = SSMOccurrenceCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def ssm_occurrence_centric_index(sqlContext, maf_df):
    """ Generates a ssm_occurrence centric index for testing """
    es = T.generate_index(sqlContext, maf_df)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def ssm_occurrence_stats(ssm_occurrence_centric_index):
    docs = ssm_occurrence_centric_index.search(index=conf.indices['ssm_occurrence_centric'],
                                  doc_type='ssm_occurrence_centric',
                                  body={"query": {"match_all": {}}}, size=1000)
    yield SSMOcurrenceCentricStats(docs['hits']['hits'])


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
def test_ssm_occurrence_centric_summary_stats(ssm_occurrence_stats,
                                              maf_stats, stat):
    ssm_occ_stat = getattr(ssm_occurrence_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    assert ssm_occ_stat == maf_stat


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_occurrence_centric_join(ssm_occurrence_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_occurrence_centric_index, filename)

    # Top level keys check
    assert set(es_doc.keys()) == set(true_doc.keys())

    # Join cardinality check
    diffs = validate_two_nested_jsons_joining("ssm_occurrence{0}{1}".format(
                                                KEY_VALUE_SEPARATOR, filename),
                                              es_doc, true_doc)
    assert diffs == []


@pytest.mark.skipif(conf.skip_in_depth_tests,
                    reason="Only test after having correct data")
@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_occurrence_centric_in_depth(ssm_occurrence_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_occurrence_centric_index, filename)
    diffs = validate_two_nested_jsons("ssm_occurrence{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc, true_doc)

    print '\n\n {} {}'.format(len(diffs), builder.index_name)

    if diffs:
        diffs.append(__gathering_statistic_info(diffs))

    DiffsReporter.report_diffs(diffs, builder.index_name)

    assert diffs == []

