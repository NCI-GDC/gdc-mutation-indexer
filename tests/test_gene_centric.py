import os
import pytest
import json
import yaml
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig
from utils import match_json_structure, flatten_json


from exports.builders import GeneCentricBuilder, MAFBuilder, GeneModelBuilder

conf = TestConfig()
OUTPUT_DIR = os.path.join(conf.data_dir, 'output', 'gene_centric')
GENE = 'ENSG00000074755'  # only used in field_by_field test


@pytest.yield_fixture(scope='module')
def gene_centric_index(sqlContext, test_index):
    """ Generates a gene centricindex for testing """
    es = Elasticsearch(conf.es_host, port=conf.es_port)

    print  "\nBuilding MAF..."
    df = MAFBuilder(conf, sqlContext).build()
    print df.count()

    print "\nBuilding GeneCentric..."
    gc = GeneCentricBuilder(conf, sqlContext).build(df)

    print "\nLoading GeneCentric..."
    gc.load()

    print "\nSuccess!"
    yield es

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices['gene_centric'], ignore=399)


@pytest.fixture
def get_docs_to_compare(gene_centric_index, filename):
    index = conf.indices['gene_centric']

    # Compare each true output document with document in ES:
    with open(os.path.join(OUTPUT_DIR, filename), 'r') as f:
        true_doc = json.loads(f.read())
        query = {'query': {'match': {'gene_id': filename}}}

    es_doc = gene_centric_index.search(index=index, body=query)['hits']['hits'][0]['_source']

    return true_doc, es_doc


@pytest.fixture
def get_one_gene_fields(gene_id):
    with open(os.path.join(OUTPUT_DIR, GENE), 'r') as f:
        true_doc = json.loads(f.read())

    true_doc = flatten_json(true_doc)
    return true_doc.keys()


@pytest.mark.parametrize('filename', os.listdir(os.path.join(conf.data_dir,
                                                             'output',
                                                             'gene_centric')))
def test_gene_centric_formal(gene_centric_index, filename):
    true_doc, es_doc = get_docs_to_comptare(gene_centric_index, filename)
    assert es_doc == true_doc


@pytest.mark.parametrize('filename', os.listdir(os.path.join(conf.data_dir,
                                                             'output',
                                                             'gene_centric')))
def test_gene_centric_flat(gene_centric_index, filename):
    true_doc, es_doc = map(flatten_json, get_docs_to_compare(gene_centric_index, filename))

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
    print cnt
    import pdb
    pdb.set_trace()
    assert es_doc == true_doc


@pytest.mark.parametrize('field', get_one_gene_fields(GENE))
def test_gene_centric_field_by_field(gene_centric_index, field):
    true_doc, es_doc = map(flatten_json, get_docs_to_compare(gene_centric_index, GENE))
    assert true_doc[field] == es_doc[field]


@pytest.mark.mytest
def test_structure():
    index_number = int(conf.indices['gene_centric'].split('_')[1][1:]) - 1
    # index_number = 0
    index = "gdc_r{}_test_gene_centric__".format(index_number)

    es = Elasticsearch(conf.es_host, port=conf.es_port)

    # Compare each true output document with document in ES:
    for filename in  os.listdir(OUTPUT_DIR):
        with open(os.path.join(OUTPUT_DIR, filename), 'r') as f:
            true_doc = json.loads(f.read())

            query = {'query': {'match': {'gene_id': filename}}}
            es_doc = es.search(index=index, body=query)['hits']['hits'][0]['_source']

            print '\nGene name match:', es_doc['gene_id'] == true_doc['gene_id']

            if set(true_doc.keys()) != set(es_doc.keys()):
                print "\nKeys mismatch:"
                print 'True not in ES', set(true_doc.keys()) - set(es_doc.keys())
                print 'ES not in True', set(es_doc.keys()) - set(true_doc.keys())
            else:
                print "{} first level okay".format(filename)
                match_json_structure(true_doc, es_doc)
                import pdb
                pdb.set_trace()
