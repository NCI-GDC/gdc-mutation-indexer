import os
import unittest
import pytest
import json
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig
from utils import match_json_structure, flatten_json

from exports.builders import SSMCentricBuilder, MAFBuilder

conf = TestConfig()

BUILDER = SSMCentricBuilder
INDEX = 'ssm_centric'
ID_FIELD = 'ssm_id'
OUTPUT_DIR = os.path.join(conf.data_dir, 'output', INDEX)
DOC = '086f02a5-2702-580b-afa6-2ac0dedaa7f5'  # only used in field_by_field test


@pytest.yield_fixture(scope='module')
def ssm_centric_index(sqlContext, test_index):
    ''' Generates a ssm centric index for testing '''
    es = Elasticsearch(conf.es_host, port=conf.es_port)
    #print es.indices.get_alias().keys()

    r = es.indices.create(index=conf.indices[INDEX], ignore=400)

    print 'Building MAF...'
    df = MAFBuilder(conf, sqlContext).build()
    print 'Building {}...'.format(INDEX)
    BUILDER(conf, sqlContext).build(df).load()

    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[INDEX], ignore=399)


@pytest.fixture
def get_docs_to_compare(ssm_centric_index, filename):
    index = conf.indices[INDEX]

    # Compare each true output document with document in ES:
    with open(os.path.join(OUTPUT_DIR, filename), 'r') as f:
        true_doc = json.loads(f.read())
        query = {'query': {'match': {ID_FIELD: filename}}}

    es_doc = ssm_centric_index.search(index=index, body=query)['hits']['hits'][0]['_source']

    return true_doc, es_doc


@pytest.fixture
def get_one_doc_fields(doc_id):
    with open(os.path.join(OUTPUT_DIR, doc_id), 'r') as f:
        true_doc = json.loads(f.read())

    true_doc = flatten_json(true_doc)
    return true_doc.keys()


@pytest.mark.parametrize('filename', os.listdir(OUTPUT_DIR))
def test_ssm_centric_formal(ssm_centric_index, filename):
    true_doc, es_doc = get_docs_to_comptare(ssm_centric_index, filename)
    assert es_doc == true_doc


@pytest.mark.parametrize('filename', os.listdir(OUTPUT_DIR))
def test_ssm_centric_flat(ssm_centric_index, filename):
    true_doc, es_doc = map(flatten_json, get_docs_to_compare(ssm_centric_index, filename))

    cnt = {'correct': 0, 'missing_fields': 0, 'wrong_values': 0, 'total': len(true_doc.keys())}
    err = {'missing_fields': [], 'wrong_values_for': [], 'wrong_values': []}
    for k, v in true_doc.items():
        if k not in es_doc:
            print "[Missing field]: P{}".format(k)
            cnt['missing_fields'] += 1
            err['missing_fields'].append(k)
        elif es_doc[k] != v:
            print "[Value mismatch]: {} |Not Equals| {} [{}]".format(v, es_doc[k], k)
            cnt['wrong_values'] += 1
            err['wrong_values'].append([v, es_doc[k]])
            err['wrong_values_for'].append(k)
        else:
            cnt['correct'] += 1

    print "\nStats: {}".format(cnt)
    print "Correctness: {}%\n".format(float(cnt['correct'])/cnt['total'])
    import pdb
    pdb.set_trace()
    assert es_doc == true_doc


@pytest.mark.parametrize('field', get_one_doc_fields(DOC))
def test_ssm_centric_field_by_field(ssm_centric_index, field):
    true_doc, es_doc = map(flatten_json, get_docs_to_compare(ssm_centric_index, DOC))
    assert true_doc[field] == es_doc[field]


@pytest.mark.mytest
def test_structure():
    index_number = int(conf.indices[INDEX].split('_')[1][1:]) - 1
    # index_number = 0
    index = "gdc_r{}_test_{}__".format(index_number, INDEX)

    es = Elasticsearch(conf.es_host, port=conf.es_port)

    # Compare each true output document with document in ES:
    for filename in  os.listdir(OUTPUT_DIR):
        with open(os.path.join(OUTPUT_DIR, filename), 'r') as f:
            true_doc = json.loads(f.read())

            query = {'query': {'match': {ID_FIELD: filename}}}
            es_doc = es.search(index=index, body=query)['hits']['hits'][0]['_source']

            print '\n{} match: {}'.format(ID_FIELD, es_doc[ID_FIELD] == true_doc[ID_FIELD])

            if set(true_doc.keys()) != set(es_doc.keys()):
                print "\nKeys mismatch:"
                print 'True not in ES', set(true_doc.keys()) - set(es_doc.keys())
                print 'ES not in True', set(es_doc.keys()) - set(true_doc.keys())
            else:
                print "{} first level okay".format(filename)
                match_json_structure(true_doc, es_doc)
                import pdb
                pdb.set_trace()
