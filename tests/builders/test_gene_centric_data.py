import os
import pytest

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import GeneCentricBuilder
from utils.json_metrics import GeneCentricStats
from utils.json_test_utils import (
                                   validate_two_nested_jsons_joining,
                                   validate_two_nested_jsons,
                                   __gathering_statistic_info,
                                   KEY_VALUE_SEPARATOR,
                                   DiffsReporter,
                                   )

builder = GeneCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def gene_centric_index(sqlContext, maf_df):
    """ Generates a gene centric index for testing """
    es = T.generate_index(sqlContext, maf_df)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def gene_stats(gene_centric_index):
    docs = gene_centric_index.search(index=conf.indices['gene_centric'],
                                     doc_type='gene_centric',
                                     body={"query": {"match_all": {}}},
                                     size=1000)
    yield GeneCentricStats(docs['hits']['hits'])


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
@pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
def test_gene_centric_summary_stats(gene_stats, maf_stats, stat):
    gene_stat = getattr(gene_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    assert gene_stat == maf_stat

