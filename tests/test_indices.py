import os
import pytest
import json
import yaml
from jsonpath_rw import parse
from elasticsearch import Elasticsearch

from conftest import get_validation_paths
from config import TestConfig
from utils import match_json_structure, flatten_json


from exports.builders import (MAFBuilder,
                              GeneCentricBuilder, SSMCentricBuilder,
                              CaseCentricBuilder, SSMOccurrenceCentricBuilder)

conf = TestConfig()

ID_FIELDS = {'gene_centric': 'gene_id',
             'case_centric': 'case_id',
             'ssm_centric': 'ssm_id',
             'ssm_occurrence_centric': 'ssm_occurrence_id'}


class IndexInfo:
    def __init__(self, builder):
        self.Builder = builder
        self.index = conf.indices[builder.index_name]
        self.output_dir = os.path.join(conf.data_dir, 'output', builder.index_name)
        self.output_files = os.listdir(self.output_dir)
        self.id_field = ID_FIELDS[builder.index_name]

    # def gen_true_docs(self):
    #     for filename in self.output_files:
    #         with open(os.path.join(self.output_dir, filename), 'r') as f:
    #             doc = json.loads(f.read())
    #         yield doc


@pytest.yield_fixture(params=[GeneCentricBuilder,
                              CaseCentricBuilder,
                              SSMCentricBuilder,
                              SSMOccurrenceCentricBuilder], scope='module')
def build_index(sqlContext, test_index, request):
    es = Elasticsearch(conf.es_host, port=conf.es_port)

    Builder = request.param

    # print  "\n[{}] Building MAF...".format(Builder.index_name)
    # df = MAFBuilder(conf, sqlContext).build()
    # print df.count()

    # print "\n[{}] Building...".format(Builder.index_name)
    # df = Builder(conf, sqlContext).build(df)

    # print "\n[{}] Loading...".format(Builder.index_name)
    # df.load()
    # print "\n[{}] Loaded to ES successfully".format(Builder.index_name)

    yield es, Builder

    if not conf.keep_indices:
        es.indices.delete(index=conf.indices[Builder.index_name], ignore=399)


@pytest.fixture()
def get_docs_to_compare(build_index, request):
    es, Builder = build_index
    print Builder.index_name
    print request.param, '!'
    true_doc = {'a': {'b': 1}, 'c':[2]}
    es_doc = {'a': {'b': 1}, 'c':[2]}
    yield true_doc, es_doc


# class TestIndices:
#     def test_indices(get_docs_to_compare):
#         true_doc, es_doc = get_docs_to_compare
#         print true_doc, es_doc
#
#     def test_gene_centric():
#         pass
#     def test_case_centric():
#         pass
#
