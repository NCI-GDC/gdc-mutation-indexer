import os
import csv
import json
import argparse
import requests

from elasticsearch import Elasticsearch

from config import TestConfig

def case_uuids_from_maf(maf):
    '''
    Get case docs from a maf containing case barcodes
    '''
    submitter_ids = []
    with open(os.path.join(TestConfig.data_dir, maf), 'rb') as f:
        next(f)
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if 'Tumor_Sample_Barcode' in row:
                submitter_ids.append(row["Tumor_Sample_Barcode"][:12])

    filt = {
        "op":"in",
            "content":{
                "field": "submitter_id",
                "value": submitter_ids
            }
    }

    params = {
        'filters':json.dumps(filt),
    }

    resp = requests.get('https://gdc-api.nci.nih.gov/cases', params=params).json()
    resp = resp['data']['hits']

    case_uuids = set( r['case_id'] for r in resp )
    return case_uuids

def main(args):
    uuids = case_uuids_from_maf(os.path.join(TestConfig.data_dir, 'kirp.muse.test.maf'))
    uuids = uuids.union(case_uuids_from_maf(os.path.join(TestConfig.data_dir, 'kirp.mutect.test.maf')))

    print uuids
    es = Elasticsearch(hosts=[args.gdc_es_host],
                       http_auth=(args.gdc_es_user,args.gdc_es_pass),
                       timeout=9999)

    # These two cases were found such that they each share at least one gene
    case_docs = es.mget(index='gdc_from_graph',
                      doc_type='case',
                      body={'ids':list(uuids)})

    print 'Got {} case docs'.format(len(case_docs['docs']))
    with open(os.path.join(args.output_path, 'cases.json'), 'w') as f:
        json.dump(case_docs, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gdc-es-host', type=str,
                        help='Elasticsearch host for the graph index',
                        default=os.environ.get('GDC_ES_HOST', None))
    parser.add_argument('--gdc-es-user', type=str,
                        help='Elasticsearch user for the graph index',
                        default=os.environ.get('GDC_ES_USER', None))
    parser.add_argument('--gdc-es-pass', type=str,
                        help='Elasticsearch password for the graph index',
                        default=os.environ.get('GDC_ES_PASS', None))
    parser.add_argument('--output-path', type=str, default='tests/data/',
                        help='Output path for test documents')

    args = parser.parse_args()
    main(args)
