import os
import pytest
import yaml
import json

from conftest import get_validation_paths
from config import TestConfig
from utils import SparkTestCase

from exports.builders import MAFBuilder

conf = TestConfig()
doc_cache = {}


def get_schema_fields(tree, fields_list):
    stack = tree.items()
    while stack:
        k, v = stack.pop()

        if k == "name":
            fields_list.append(v) 

        if isinstance(v, dict):
            stack.extend(v.iteritems())

        elif isinstance(v, list):
            for d in v:
                stack.extend(d.iteritems())

    return fields_list
       

def get_mapping_fields(tree, fields_list):
    if not isinstance(tree, dict):
        return fields_list

    for key in tree:
        if key == 'properties':
            fields_list.extend(tree[key].keys())
        get_mapping_fields(tree[key], fields_list)

    return fields_list


def if_schema_correct(schema_tree, yaml_file):
    with open(yaml_file, 'r') as f:
        tree = yaml.load(f.read())
        yml_fields = get_mapping_fields(tree, [])
        maf_fields = get_schema_fields(schema_tree, [])

    set_diff = set(yml_fields) - set(maf_fields)

    return len(set_diff) == 0


class TestMAFGeneModelJoin(SparkTestCase):

    def test_maf_gene_join_contains(self):
        files_blacklist = ['common_settings.yml', 'case.yml']

        mappings_dir = os.path.join(os.path.dirname(conf.test_dir),
                                    "exports", "mappings")
    
        joined = MAFBuilder(conf, self.sqlContext).build()
        schema = json.loads(joined.schema.json())
