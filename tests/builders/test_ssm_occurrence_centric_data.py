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

