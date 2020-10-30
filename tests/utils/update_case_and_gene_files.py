import json
import gzip
import sys
import os
import boto
import boto.s3.connection
from elasticsearch import Elasticsearch
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_dir)
from tests_config import TestConfig
from config import factory

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
    print('- Downloading full gene model')
    gene_model = get_full_gene_model()

    print('- Extracting set of genes from test mafs')
    genes_to_keep = get_unique_from_mafs('Gene')

    print('- Dropping extra genes from gene model')
    gene_model = [gene for gene in gene_model if gene['_gene_id'] in genes_to_keep]

    filepath = os.path.join(cfg_test.test_dir, 'data', 'input', 'genes.json')
    print('- Writing gene model to {}'.format(filepath + '.gz'))
    write_to_file(gene_model, filepath)


def update_cases(es):
    """
    Updates cases.json.gz file according to cases that are present in test mafs

    - Get a set of cases that appear in test mafs
    - Get corresponding case documents from gdc_from_graph
    - Save results to tests/data/input/cases.json.gz

    """

    print('- Extracting aliquots from headers')
    aliquots_in_headers = get_all_aliquots_from_mafs()
    aliquots_in_data = get_unique_from_mafs('Tumor_Sample_Barcode')

    if aliquots_in_data - aliquots_in_headers != set():
        raise Exception('Aliquots missing from headers: {}'
                        .format(aliquots_in_data - aliquots_in_headers))
    if aliquots_in_headers - aliquots_in_data == set():
        raise Exception('No empty aliquots in test data')

    print('\tEmpty aliquots: {}'.format(aliquots_in_headers - aliquots_in_data))

    print('- Getting case_ids for aliquots in maf headers')
    case_ids = get_case_ids_from_aliquots(es, aliquots_in_headers)

    print('- Getting cases data for case_ids')
    cases = get_cases(es, case_ids)

    filepath = os.path.join(cfg_test.test_dir, 'data', 'input', 'cases.json')
    print('- Writing cases to {}'.format(filepath + '.gz'))
    write_to_file(cases, filepath)


def update_files(es):

    print('- Extracting aliquots from headers')
    aliquots_in_headers = get_all_aliquots_from_mafs()
    aliquots_in_data = get_unique_from_mafs('Tumor_Sample_Barcode')

    if aliquots_in_data - aliquots_in_headers != set():
        raise Exception('Aliquots missing from headers: {}'
                        .format(aliquots_in_data - aliquots_in_headers))
    if aliquots_in_headers - aliquots_in_data == set():
        raise Exception('No empty aliquots in test data')

    print('\tEmpty aliquots: {}'.format(aliquots_in_headers - aliquots_in_data))

    print('- Getting case_ids for aliquots in maf headers')
    case_ids = get_case_ids_from_aliquots(es, aliquots_in_headers)

    print('- Getting files data for case_ids')
    files = get_files(es, case_ids)

    filepath = os.path.join(cfg_test.test_dir, 'data', 'input', 'files.json')
    print('- Writing files to {}'.format(filepath + '.gz'))
    write_to_file(files, filepath)


def get_case_ids_from_aliquots(es, aliquot_ids):
    """
    Get case_ids corresponding to aliquot_ids from graph_case
    """

    print("make it use the data from graph_case instead")

    cases = []
    for aliquot_id in aliquot_ids:
        query = {
            "query": {
                "nested": {
                    "path": 'samples.portions.analytes.aliquots',
                    "query": {
                        "bool": {
                            "must": [
                                {"match_phrase": {'samples.portions.analytes.aliquots.submitter_id': aliquot_id}},
                            ]
                        }
                    }
                }
            }
        }
        res = es.search(index='graph_case', body=query)
        assert len(res['hits']['hits']) == 1, 'Unexpected number of cases found'

        cases.append(res['hits']['hits'][0]['_source'])

    return {c['case_id'] for c in cases}


def get_cases(es, case_ids):
    """
    Get case documents from graph_case
    """
    docs = []
    for case_id in case_ids:
        doc = es.get(index='graph_case', id=case_id)['_source']
        docs.append(doc)
    return docs


def get_files(es, case_ids):
    """
    Get file documents from graph_file
    """
    docs = []
    for case_id in case_ids:
        case_doc = es.get(index='graph_file', id=case_id)['_source']
        file_docs = case_doc['files'][:2]
        docs.extend(file_docs)
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
        validate_certs=False,
        https_connection_factory=factory,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    # Gene model file:
    filename = 'genes.hg38.v2.json'

    bucket = conn.get_bucket('gdc-mutation-indexer')
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
    """
    Extracts aliquots set from maf headers
    """
    aliquots = set()
    for filepath in cfg_test.maf_urls:
        with open(filepath.replace('file://', ''), 'r') as f:
            for line in f.readlines():
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
    print('\n\tUpdating cases.json.gz:')
    update_cases(es)
    print('\n\tUpdating genes.json.gz:')
    update_genes()
    print('\n\tUpdating files.json.gz')
    update_files(es)
