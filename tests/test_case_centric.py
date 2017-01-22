import os
import unittest
import pytest
import json
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig

from exports.builders import CaseCentricBuilder, MAFBuilder

conf = TestConfig()

@pytest.yield_fixture(scope='module')
def case_centric_index(sqlContext, test_index):
    ''' Generates a case centric index for testing '''
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    #print es.indices.get_alias().keys()

    #r = es.indices.delete(index=conf.indices['case_centric'], ignore=399)
    r = es.indices.create(index=conf.indices['case_centric'], ignore=400)

    df = MAFBuilder(conf, sqlContext).build()
    CaseCentricBuilder(conf, sqlContext).build(df).load()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices['case_centric'], ignore=399)

@pytest.mark.parametrize('doc,path',
    get_validation_paths('tests/data/case.validation.1bf54408-b5cb-45dc-ad03-ef2866a0ff59.json')
)
def test_case_doc_contains(case_centric_index, doc, path):
    ''' Test that document contains a field from a path'''
    d = case_centric_index.get(conf.indices['case_centric'],
                             doc,
                             doc_type=conf.index_names['case_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len([r.value for r in results]) > 0

@pytest.mark.parametrize('doc,path,count', [
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59', 'gene[*].gene_id', 290),
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59', 'gene[*].ssm[*].ssm_id', 344),
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59', 'gene[*].ssm[*].consequence[*].transcript.annotation.impact', 3477)
])
def test_case_path_count(case_centric_index, doc, path, count):
    d = case_centric_index.get(conf.indices['case_centric'],
                             doc,
                             doc_type=conf.index_names['case_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len(results) == count
