import os
import pytest
from deepdiff import DeepDiff

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import SSMCentricBuilder
from utils.json_validation import JSONValidator

from utils.json_metrics import SSMCentricStats

builder = SSMCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def ssm_centric_index(sqlContext, test_index):
    """ Generates a ssm centric index for testing """
    es = T.generate_index(sqlContext)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def ssm_stats(sqlContext, ssm_centric_index):
    docs = ssm_centric_index.search(index=conf.indices['ssm_centric'],
                                  body={"query": {"match_all": {}}}, size=1000)
    yield SSMCentricStats(docs['hits']['hits'])


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_centric_formal(ssm_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)

    diff = DeepDiff(es_doc, true_doc,
                    ignore_order=True, view='tree')
    T.report_deepdiff(diff)
    assert diff == {}


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_ssm_centric_flat(ssm_centric_index, filename):
    es_doc, true_doc = map(T.flatten_json,
                           T.get_docs_to_compare(ssm_centric_index, filename))

    T.report_correctness(es_doc, true_doc, label=filename)

    for k, v in true_doc.items():
        assert k in es_doc
        assert es_doc[k] == v


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
@pytest.mark.parametrize('test_mode', ['list', 'dict'])
def test_ssm_centric_cardinality(ssm_centric_index, filename, test_mode):
    es_doc, true_doc = T.get_docs_to_compare(ssm_centric_index, filename)
    mismatches = JSONValidator.find_mismatches(es_doc, true_doc, test_mode)
    T.report_cardinality(mismatches, '[{}|{}]'.format(filename, test_mode))
    assert mismatches == {}


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
def test_ssm_centric_summary_stats(ssm_centric_index, ssm_stats, maf_stats, stat):
    ssm_stat = getattr(ssm_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    assert ssm_stat == maf_stat
