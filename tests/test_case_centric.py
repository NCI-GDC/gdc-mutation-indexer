import os
import pytest
from deepdiff import DeepDiff

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import CaseCentricBuilder
from utils.json_validation import JSONValidator

from utils.json_metrics import CaseCentricStats
from json_test_utils import (validate_two_nested_jsons,
                             KEY_VALUE_SEPARATOR)

builder = CaseCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def case_centric_index(sqlContext, test_index):
    """ Generates a case centric index for testing """
    es = T.generate_index(sqlContext)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def case_stats(sqlContext, case_centric_index):
    docs = case_centric_index.search(index=conf.indices['case_centric'],
                                  body={"query": {"match_all": {}}}, size=1000)
    yield CaseCentricStats(docs['hits']['hits'])


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_formal(case_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)

    diff = DeepDiff(es_doc, true_doc,
                    ignore_order=True, view='tree')
    T.report_deepdiff(diff)
    assert diff == {}


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_case_centric_flat(case_centric_index, filename):
    es_doc, true_doc = map(T.flatten_json,
                           T.get_docs_to_compare(case_centric_index, filename))

    T.report_correctness(es_doc, true_doc, label=filename)

    for k, v in true_doc.items():
        assert k in es_doc
        assert es_doc[k] == v


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
@pytest.mark.parametrize('test_mode', ['list', 'dict'])
def test_case_centric_cardinality(case_centric_index, filename, test_mode):
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)
    mismatches = JSONValidator.find_mismatches(es_doc, true_doc, test_mode)
    T.report_cardinality(mismatches, '[{}|{}]'.format(filename, test_mode))
    assert mismatches == {}


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
def test_case_centric_summary_stats(case_centric_index, case_stats, maf_stats, stat):
    case_stat = getattr(case_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    assert case_stat == maf_stat

def test_case_centric_join(case_centric_index, filename):
    '''
    Builds case-centric dataframe given case and maf dataframes

    case{}
         |___ gene[]
                 |___ ssm[]
                       |___ consequence[]
                       |             |_____ transcript{}
                       |                          |_____ annotation{}
                       |___ observation[]
    '''
    es_doc, true_doc = T.get_docs_to_compare(case_centric_index, filename)
    diffs = validate_two_nested_jsons("case{0}{1}".format(KEY_VALUE_SEPARATOR, filename),
                                      es_doc, true_doc)
    assert diffs == []
