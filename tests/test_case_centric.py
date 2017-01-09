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

    maf_builder = MAFBuilder(conf, sqlContext)
    urls = ['file://'+os.path.join(conf.data_dir, 'kirp.mutect.test.maf'),
            'file://'+os.path.join(conf.data_dir, 'kirp.muse.test.maf')]

    df = maf_builder.combine(urls)
    df = maf_builder.standardize_schema(df)
    df = maf_builder.add_ssm_id(df)
    df = maf_builder.add_null(df)
    df = maf_builder.extract_barcode(df)

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
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59', '[*].ssm.[*].ssm_id', 5)
])
def test_path_count(case_centric_index, doc, path, count):
    d = case_centric_index.get(conf.indices['case_centric'],
                             doc,
                             doc_type=conf.index_names['case_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len(results) == count
