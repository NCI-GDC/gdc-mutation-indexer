import json
import os
import re
from unittest import mock

import pytest
import yaml
from pyspark.sql import types

from mutation_indexer.viz import builders
from mutation_indexer.viz.builders.clinical_annotations import civic
from tests.integration import config

conf = config.TestConfig()


@pytest.mark.usefixtures("sqlContext", "maf_df")
class TestMAFBuilder:
    @pytest.fixture
    def maf_schema(self):
        path = os.path.join(conf.schemas_dir, "maf.yml")
        with open(path) as f:
            maf_schema = yaml.safe_load(f)["maf_schema"]

        return maf_schema

    @pytest.fixture
    def annotation_schemas(self, sqlContext):
        builder = builders.MAFBuilder(
            conf,
            sqlContext,
            mock.MagicMock(),
            (civic.CivicBuilder(conf, sqlContext),),
        )

        return builder.get_annotation_schemas()

    def test_ssm_id(self, maf_df):
        """
        Test that ssm_id column is created
        """
        assert "ssm_id" in maf_df.columns

    def test_cosmic_id(self, maf_df):
        """
        Test that cosmic_id column is created and is ArrayType(StringType())
        """
        assert "cosmic_id" in maf_df.columns
        data_type = maf_df.schema["cosmic_id"].dataType
        assert isinstance(data_type, types.ArrayType)
        assert isinstance(data_type.elementType, types.StringType)

    def test_genomic_dna_change(self, maf_df):
        """
        Test that the genomic_dna_change is created correctly
        """

        assert "genomic_dna_change" in maf_df.columns

        labels = {
            row.genomic_dna_change
            for row in maf_df.select("genomic_dna_change").collect()
        }

        # Number of unique labels should be equal to the number of unique ssm
        assert maf_df.select("ssm_id").distinct().count() == len(labels)

        # SNPs
        assert "chr1:g.32180498T>C" in labels
        assert "chr2:g.182729892G>T" in labels
        assert "chr3:g.38112297T>G" in labels
        assert "chr9:g.2056812T>A" in labels

        # Small insertions
        assert "chr17:g.4076894_4076895insT" in labels

        # Small deletions
        assert "chr6:g.72307351delAT" in labels

        # DNPs
        assert "chr3:g.38112298_38112299delinsCA" in labels

        # TNPs
        assert "chr3:g.38112300_38112302delinsGCT" in labels

        # ONPs
        assert "chr3:g.38112303_38112306delinsGTGC" in labels

    def test_mutation_type(self, maf_df):
        """
        Test that mutation_type is created properly
        """

        assert "mutation_type" in maf_df.columns

        # Should only have 1 type, 'Simple Somatic Mutation'
        assert maf_df.select("mutation_type").distinct().count() == 1
        assert (
            maf_df.select("mutation_type").collect()[0]["mutation_type"]
            == "Simple Somatic Mutation"
        )

    def test_variant_caller(self, maf_df, raw_variant_caller_counts):
        """
        Test that variant caller is created properly
        """
        assert "variant_caller" in maf_df.columns

        actual_counts = dict(maf_df.groupBy("variant_caller").count().collect())
        assert actual_counts == raw_variant_caller_counts

    def test_variant_process(self, maf_df):
        """
        Test that variant process is created properly
        """

        assert "variant_process" in maf_df.columns
        assert maf_df.first()["variant_process"] == "masked"

    def test_mutation_subtype(self, maf_df):
        """
        Test that mutation_subtype is created properly
        """

        assert "mutation_subtype" in maf_df.columns

        # Should have as many distinct variants as subtypes
        assert (
            maf_df.select("variant_type").distinct().count()
            == maf_df.select("mutation_subtype").distinct().count()
        )

    def test_maf_field_types(self, maf_df):
        """
        Test that maf_df field types correspond to maf.yml
        """
        types = {"int": "integer", "bool": "boolean", "float": "float"}
        for col in maf_df.schema:
            col_info = json.loads(col.json())
            if col_info["name"] in maf_df.schema:
                if "type" in maf_df.schema[col_info["name"]]:
                    assert (
                        col_info["type"]
                        == types[maf_df.schema[col_info["name"]]["type"]]
                    )

    def test_maf_field_pattern(self, maf_df):
        """
        Test that maf_df field pattern correspond to maf.yml
        """
        for col in maf_df.schema:
            col_info = json.loads(col.json())
            colname = col_info["name"]
            if colname in maf_df.schema:
                if "pattern" in maf_df.schema[colname]:
                    pattern = maf_df.schema[colname]["pattern"]
                    values = maf_df.select(colname)
                    for row in values.collect():
                        val = row[colname]
                        is_matching = re.search(pattern.replace("{}", ".*"), val)
                        assert is_matching

    def test_annotations(self, annotation_schemas, maf_df):
        for schema in annotation_schemas:
            for k in schema.keys():
                assert k in maf_df.columns
