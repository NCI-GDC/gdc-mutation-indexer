import json
import os
import re

import pytest
import yaml
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.builders.viz.clinical_annotations import civic
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
            conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)
        )
        return builder.get_annotation_schemas()

    def test_patch_url(self, sqlContext):
        """Test that s3 urls are patched correctly"""
        builder = builders.MAFBuilder(
            conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)
        )
        url1 = "s3://cleversafe.service.consul/aoneuhtasoeh/aoenstuh.txt"
        assert builder.patch_url(url1).startswith("s3a://")

    def test_combine(self, sqlContext, raw_variant_caller_counts):
        """
        Test that mafs are combined correctly
        """
        builder = builders.MAFBuilder(
            conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)
        )

        combined_df = builder.combine(conf.maf_urls)

        actual_counts = dict(combined_df.groupBy("variant_caller").count().collect())
        assert actual_counts == raw_variant_caller_counts

    def test_schema(self, sqlContext, maf_schema):
        """
        Test that maf has columns correctly renamed
        """
        builder = builders.MAFBuilder(
            conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)
        )

        # combine() should standardize the columns while loading the data
        df = builder.combine(conf.maf_urls)

        for field in maf_schema.keys():
            assert field in df.columns

    def test_standardize_schema_field_order(self, sqlContext, maf_schema):
        """
        Confirm that standardize_schema standardizes the field order

        This verifies that we can safely union mafs together
        """
        builder = builders.MAFBuilder(
            conf, sqlContext, (civic.CivicBuilder(conf, sqlContext),)
        )

        # Bypass combine() so the dataframe isn't already standardized.
        # Note that this particular input DF is missing a column, which
        # we need to fix or else standardize_schema will reject it.
        df = builder.file_to_df(conf.maf_urls[0]).withColumn(
            "callers", F.lit("variant_caller")
        )
        columns = df.columns

        sort_df = builder.standardize_schema(df.select(*sorted(columns)))
        assert len(sort_df.columns) == len(maf_schema.keys())
        assert set(sort_df.columns) == maf_schema.keys()

        reverse_df = builder.standardize_schema(df.select(*reversed(columns)))
        assert reverse_df.columns == sort_df.columns

        extra_columns = columns[:]
        extra_columns.insert(0, F.lit("asdf").alias("extra"))
        extra_columns.insert(4, F.lit(300).alias("extraneous"))
        extra_columns.append(F.lit(None).alias("superfluous"))
        extra_df = builder.standardize_schema(df.select(*extra_columns))
        assert extra_df.columns == sort_df.columns

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
