import os
import pytest
import json
import yaml
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig

from exports.builders import GeneCentricBuilder, MAFBuilder

conf = TestConfig()
doc_cache = {}
GENE_ID = None
# GENE_ID = 'ENSG00000092931'  # <= OLD one


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
    gc.load(did=GENE_ID)

    print "\nSuccess!"
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
                         get_validation_paths(
                             'tests/data/gene.validation.ENSG00000092931.json'))
def test_gene_doc_contains(gene_centric_index, doc, path):
    """ Test that document contains a field from a path """
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
    (GENE_ID, 'case[*].case_id', 1),
    (GENE_ID, 'case[*].ssm[*].ssm_id', 119),
    (GENE_ID, 'case[*].ssm[*].consequence[*].transcript.annotation.impact', 3477)
])
def test_path_count(gene_centric_index, doc, path, count):
    d = gene_centric_index.get(conf.indices['gene_centric'],
                               doc,
                               doc_type=conf.index_names['gene_centric'])
    d = d['_source']
    results = parse(path).find(d)
    assert len(results) == count


def test_gene_structure(gene_centric_index):
    print "\nGENE STRUCTURE TEST"
    print 'Test docs here'


@pytest.yield_fixture(scope='module')
def sql_context(sqlContext):
    yield sqlContext


def test_maf_schema(sql_context):
    test_dir = conf.test_dir

    data_dir = os.path.join(test_dir, 'data')
    input_dir = os.path.join(data_dir, 'input')

    maf_dir = os.path.join(input_dir, 'maf')

    old_files = ['file://' + os.path.join(data_dir, 'kirp.mutect.test.maf'),
                 'file://' + os.path.join(data_dir, 'kirp.muse.test.maf')]

    new_files = ['file://' + os.path.join(maf_dir, f)
                 for f in os.listdir(maf_dir) if f.split('.')[-1] == 'maf']

    conf.maf_urls = old_files
    old_maf = MAFBuilder(conf, sql_context).build()

    conf.maf_urls = new_files
    new_maf = MAFBuilder(conf, sql_context).build()

    assert new_maf.schema == old_maf.schema
    assert len(set(old_maf.columns) - set(new_maf.columns)) == 0
    assert len(set(new_maf.columns) - set(old_maf.columns)) == 0


def test_mytest():
    index_number = int(conf.indices['gene_centric'].split('_')[1][1:]) - 1
    # index_number = 0
    index = "gdc_r{}_test_gene_centric__".format(index_number)


    es = Elasticsearch(conf.es_host, port=conf.es_port)

    output_dir = os.path.join(conf.data_dir, 'output')

    # Compare each true output document with document in ES:
    for filename in  os.listdir(output_dir):
        with open(os.path.join(output_dir, filename), 'r') as f:
            true_doc = json.loads(f.read())

            query = {'query': {'match': {'gene_id': filename}}}
            es_doc = es.search(index=index, body=query)['hits']['hits'][0]['_source']

            print '\nGene name match:', es_doc['gene_id'] == true_doc['gene_id']

            if not true_doc.keys() == es_doc.keys():
                print "\nKeys mismatch:"
                print 'True not in ES', set(true_doc.keys()) - set(es_doc.keys())
                print 'ES not in True', set(es_doc.keys()) - set(true_doc.keys())

    import pdb
    pdb.set_trace()

