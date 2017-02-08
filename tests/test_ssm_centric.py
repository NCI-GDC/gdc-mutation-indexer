import os
import unittest
import pytest
import json
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig

from exports.builders import SSMCentricBuilder, MAFBuilder

conf = TestConfig()

@pytest.yield_fixture(scope='module')
def ssm_centric_index(sqlContext, test_index):
    ''' Generates a ssm centric index for testing '''
    es = Elasticsearch(conf.es_host, port=conf.es_port)

    r = es.indices.create(index=conf.indices['ssm_centric'], ignore=400)

    df = MAFBuilder(conf, sqlContext).build()
    SSMCentricBuilder(conf, sqlContext).build(df).load()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices['ssm_centric'], ignore=399)

@pytest.mark.parametrize('doc,path',
    get_validation_paths('tests/data/ssm.validation.3dccd994-67f5-577f-8bca-a95269303af5.json')
)
def test_ssm_doc_contains(ssm_centric_index, doc, path):
    ''' Test that document contains a field from a path'''
    d = ssm_centric_index.get(conf.indices['ssm_centric'],
                             doc,
                             doc_type=conf.index_names['ssm_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len([r.value for r in results]) > 0

@pytest.mark.parametrize('doc,path,count', [
    ('0c7e4bf7-e3bf-5363-80b4-dc42873b3534', '[*].ssm.[*].ssm_id', 5)
])
def test_path_count(ssm_centric_index, doc, path, count):
    d = ssm_centric_index.get(conf.indices['ssm_centric'],
                             doc,
                             doc_type=conf.index_names['ssm_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len(results) == count
