import os
import pytest
from deepdiff import DeepDiff

from tests_config import TestConfig
from base_index_test import BaseIndexTest
from exports.builders import GeneCentricBuilder
from utils.json_validation import JSONValidator

from utils.json_metrics import GeneCentricStats

from utils.json_metrics import GeneCentricStats

builder = GeneCentricBuilder
conf = TestConfig()
T = BaseIndexTest(builder, conf)


@pytest.yield_fixture(scope='module')
def gene_centric_index(sqlContext, test_index):
    """ Generates a gene centric index for testing """
    es = T.generate_index(sqlContext)

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[T.index], ignore=399)


@pytest.yield_fixture(scope='module')
def gene_stats(sqlContext, gene_centric_index):
    docs = gene_centric_index.search(index=conf.indices['gene_centric'],
                                  body={"query": {"match_all": {}}}, size=1000)
    yield GeneCentricStats(docs['hits']['hits'])

@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_gene_centric_formal(gene_centric_index, filename):
    es_doc, true_doc = T.get_docs_to_compare(gene_centric_index, filename)

    diff = DeepDiff(es_doc, true_doc,
                    ignore_order=True, view='tree')
    T.report_deepdiff(diff)
    assert diff == {}


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
def test_gene_centric_flat(gene_centric_index, filename):
    es_doc, true_doc = map(T.flatten_json,
                           T.get_docs_to_compare(gene_centric_index, filename))

    T.report_correctness(es_doc, true_doc, label=filename)

    for k, v in true_doc.items():
        assert k in es_doc
        assert es_doc[k] == v


@pytest.mark.parametrize('filename', os.listdir(T.output_dir))
@pytest.mark.parametrize('test_mode', ['list', 'dict'])
def test_gene_centric_cardinality(gene_centric_index, filename, test_mode):
    es_doc, true_doc = T.get_docs_to_compare(gene_centric_index, filename)
    mismatches = JSONValidator.find_mismatches(es_doc, true_doc, test_mode)
    T.report_cardinality(mismatches, '[{}|{}]'.format(filename, test_mode))
    assert mismatches == {}


@pytest.mark.parametrize('stat', ['Nprojects',
                                  'Ncases',
                                  'Ngenes',
                                  'NUniqMut',
                                  'Nconseq'])
def test_gene_centric_summary_stats(gene_centric_index, gene_stats, maf_stats, stat):
    gene_stat = getattr(gene_stats, stat)
    maf_stat = getattr(maf_stats, stat)
    assert gene_stat == maf_stat
