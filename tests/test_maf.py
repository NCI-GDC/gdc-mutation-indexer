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


class TestMAFGeneModelJoin(SparkTestCase):
    @staticmethod
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

    @classmethod
    def get_mapping_fields(cls, tree, fields_list):
        if not isinstance(tree, dict):
            return fields_list

        for key in tree:
            if key == 'properties':
                fields_list.extend(tree[key].keys())

            cls.get_mapping_fields(tree[key], fields_list)
        return fields_list

    @classmethod
    def if_schema_correct(cls, schema_tree, yaml_file):
        with open(yaml_file, 'r') as f:
            tree = yaml.load(f.read())
            yml_fields = cls.get_mapping_fields(tree, [])
            maf_fields = cls.get_schema_fields(schema_tree, [])

        set_diff = set(yml_fields) - set(maf_fields)

        print "\t"
        if len(set_diff) != 0:
            print list(set_diff), len(set_diff)

        return len(set_diff) == 0

    def test_maf_gene_join_contains(self):
        files_blacklist = ['common_settings.yml', 'case.yml']

        mappings_dir = os.path.join(os.path.dirname(conf.test_dir),
                                    "exports", "mappings")

        joined = MAFBuilder(conf, self.sqlContext).build()
        schema = json.loads(joined.schema.json())

        for f in os.listdir(mappings_dir):
            if f.split(".")[-1] == "yml" and f not in files_blacklist:
                is_correct = self.if_schema_correct(schema, os.path.join(mappings_dir, f))
                print f, is_correct
                print ""
                assert is_correct

    def test_maf_schema(self):
        test_dir = conf.test_dir

        data_dir = os.path.join(test_dir, 'data')
        input_dir = os.path.join(data_dir, 'input')

        maf_dir = os.path.join(input_dir, 'maf')

        old_files = ['file://' + os.path.join(data_dir, 'kirp.mutect.test.maf'),
                     'file://' + os.path.join(data_dir, 'kirp.muse.test.maf')]

        new_files = ['file://' + os.path.join(maf_dir, f)
                     for f in os.listdir(maf_dir) if f.split('.')[-1] == 'maf']

        conf.maf_urls = old_files
        old_maf = MAFBuilder(conf, self.sqlContext).build()

        conf.maf_urls = new_files
        new_maf = MAFBuilder(conf, self.addCleanup).build()

        assert new_maf.schema == old_maf.schema
        assert len(set(old_maf.columns) - set(new_maf.columns)) == 0
        assert len(set(new_maf.columns) - set(old_maf.columns)) == 0

