import os
import unittest
import pytest
import json
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig

from exports.builders import GeneCentricBuilder, MAFBuilder

conf = TestConfig()
doc_cache = {}

@pytest.yield_fixture(scope='module')
def gene_centric_index(sqlContext, test_index):
    ''' Generates a gene centricindex for testing '''
    es = Elasticsearch(conf.es_host, port=conf.es_port)

    r = es.indices.create(index=conf.indices['gene_centric'], ignore=400)

    maf_builder = MAFBuilder(conf, sqlContext)
    urls = ['file://'+os.path.join(conf.data_dir, 'kirp.mutect.test.maf'),
            'file://'+os.path.join(conf.data_dir, 'kirp.muse.test.maf')]

    df = maf_builder.combine(urls)
    df = maf_builder.standardize_schema(df)
    df = maf_builder.add_ssm_id(df)
    df = maf_builder.add_null(df)
    df = maf_builder.extract_barcode(df)

    GeneCentricBuilder(conf, sqlContext).build(df).load(did='ENSG00000092931')

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices['gene_centric'], ignore=399)

def flatten_json(d):
    flat = {}

    def flatten(doc, name=''):
        if type(doc) is dict:
            for k,v in doc.items():
                flatten(v, name+'.'+k)
        elif type(doc) is list:
            for v in doc:
                flatten(v, name)
        else:
            flat[name] = doc
    flatten(d)
    return [k for k in sorted(flat.keys(), key=lambda x: len(x)) if 'files' not in k]

@pytest.mark.parametrize('doc,path',
    get_validation_paths('tests/data/gene.validation.ENSG00000092931.json')[:1]
)
def test_gene_doc_contains(gene_centric_index, doc, path):
    ''' Test that document contains a field from a path'''
    if doc not in doc_cache:
        d = gene_centric_index.get(conf.indices['gene_centric'],
                                 doc,
                                 doc_type=conf.index_names['gene_centric'])
        doc_cache[doc] = d
    else:
        d = doc_cache[doc]
    d = d['_source']
    #print json.dumps(flatten_json(d), indent=2)
    results = parse(path).find(d)
    assert len([r.value for r in results]) > 0

@pytest.mark.parametrize('doc,path,count', [
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59', '[*].ssm.[*].ssm_id', 5)
])
def test_path_count(gene_centric_index, doc, path, count):
    d = gene_centric_index.get(conf.indices['gene_centric'],
                             doc,
                             doc_type=conf.index_names['gene_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len(results) == count
