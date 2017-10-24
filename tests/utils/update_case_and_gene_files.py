import json
import gzip
import sys
import os
import boto
import boto.s3.connection
import requests
from urllib import quote_plus
from elasticsearch import Elasticsearch

root_dir = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..'))
sys.path.append(root_dir)
from tests_config import TestConfig

cfg_test = TestConfig()

ES_HOST = os.environ['ES_HOST']
ES_PORT = os.environ['ES_PORT']
ES_USER = os.environ['ES_USER']
ES_PASSWORD = os.environ['ES_PASSWORD']

es = Elasticsearch(host=ES_HOST, http_auth=(ES_USER, ES_PASSWORD), port=ES_PORT)

S3_HOST = os.environ['S3_HOST']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']


def update_genes():
    """
    Updates gene model file genes.json.gz according to genes that are present in test mafs
    
    - Get a set of genes that appear in test mafs
    - Get full gene model from cleversafe
    - Drop all genes that not in test mafs from gene model
    - Save resulting gene model to tests/data/input/genes.json.gz

    """
    print '- Downloading full gene model'
    gene_model = get_full_gene_model()

    print '- Extracting set of genes from test mafs'
    genes_to_keep = get_unique_from_mafs('Gene')

    print '- Dropping extra genes from gene model'
    gene_model = [gene for gene in gene_model if gene['_gene_id'] in genes_to_keep]

    filepath = os.path.join(cfg_test.test_dir, 'data', 'input', 'genes.json')
    print '- Writing gene model to {}'.format(filepath + '.gz') 
    write_to_file(gene_model, filepath) 


def update_cases(es):
    """
    Updates cases.json.gz file according to cases that are present in test mafs
   
    - Get a set of cases that appear in test mafs
    - Get corresponding case documents from gdc_from_graph
    - Save results to tests/data/input/cases.json.gz

    """

    print '- Extracting set of cases from test mafs'
    cases_to_keep = get_unique_from_mafs('case_id')

    print '- Extracting aliquots from headers'
    aliquots_in_headers = get_all_aliquots_from_mafs()
    aliquots_in_data = get_unique_from_mafs('Tumor_Sample_Barcode')

    if aliquots_in_data - aliquots_in_headers != set():
        raise Exception('Aliquots missing from headers: {}'
                        .format(aliquots_in_data - aliquots_in_headers))
    if aliquots_in_headers - aliquots_in_data == set():
        raise Exception('No empty aliquots in test data')

    print '\tEmpty aliquots: {}'.format(aliquots_in_headers - aliquots_in_data)

    print '- Getting case_ids for aliquots in maf headers'
    case_ids = get_case_ids_from_aliquots(es, aliquots_in_headers)
    print '- Getting cases data for case_ids'
    cases = get_cases(es, case_ids)
    
    filepath = os.path.join(cfg_test.test_dir, 'data', 'input', 'cases.json')
    print '- Writing cases to {}'.format(filepath + '.gz') 
    write_to_file(cases, filepath) 


def get_case_ids_from_aliquots(es, aliquot_ids):
    """
    Get case_ids corresponding to aliquot ids from gdcapi
    """
    n_expected = len(aliquot_ids)
    
    query = {
       "op": "in",
       "content":{
          "field": "samples.portions.analytes.aliquots.submitter_id",
          "value": list(aliquot_ids)
        }
    }
    url = 'https://api.gdc.cancer.gov/cases/?size={}&filters='.format(n_expected + 1) + quote_plus(json.dumps(query))
    response = requests.get(url).json()['data']
    
    if response['pagination']['total'] != n_expected:
        raise Exception('Wrong number of cases. Expected {}, got {}'
                        .format(n_expected, response['pagination']['total']))

    return {c['case_id'] for c in response['hits']}


def get_cases(es, case_ids):
    """
    Get case documents from gdc_from_graph
    """
    
    docs = []
    for case_id in case_ids:
        doc = es.get(index='gdc_from_graph', doc_type='case', id=case_id)['_source']
        docs.append(doc)
    return docs


def get_full_gene_model():
    """
    Downloads full gene model from S3
    Returns gene json generator
    """
    conn = boto.connect_s3(
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
        host=S3_HOST,
        is_secure=True,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    # Gene model file:
    filename = 'genes.hg38.v2.json'

    bucket = conn.get_bucket('test')
    key = bucket.get_key(filename)
    gene_model = key.get_contents_as_string()    

    return (json.loads(gene) for gene in gene_model.split('\n') if gene != '')


def get_unique_from_mafs(column):
    """
    Returns set of unique column values extracted from test mafs 
    """
    values = set()
    for filepath in cfg_test.maf_urls:
        with open(filepath.replace('file://', ''), 'r') as f:
            for line in f.readlines():
                line_values = line.split('\t')
                if line[0] == '#':
                    continue
                elif line_values[0] == 'Hugo_Symbol':
                    col_id = [i for i, v in enumerate(line_values) if v == column][0]
                else:
                    values.update([line_values[col_id]])
    return values


def get_all_aliquots_from_mafs():
    aliquots = set()
    for filepath in cfg_test.maf_urls:
        with open(filepath.replace('file://', ''), 'r') as f:
            for line in f.readlines():
                line_values = line.split('\t')
                if line.find('#n.analyzed.samples') != -1:
                    n_samples = int(line.split()[1])
                elif line.find('#tumor.aliquots.submitter_id') != -1:
                    al = line.split()[1].split(',')
                    if n_samples != len(al):
                        raise Exception(
                            'Header is inconsistent: '
                            'n.analyzed.samples != len(tumor.aliquots.submitter_id) '
                            '({} != {})'.format(n_samples, len(al))
                        )
                    aliquots.update(al)
                    break
    return aliquots


def write_to_file(data, filepath):
    """
    Writes list of dicts data to .gz file
    """
    content = '\n'.join([json.dumps(l) for l in data])
    with gzip.open(filepath + '.gz', 'wb') as f:
        f.write(content)


if __name__ == '__main__':
    print '\n\tUpdating cases.json.gz:'
    update_cases(es)
    print '\n\tUpdating genes.json.gz:'
    update_genes()

