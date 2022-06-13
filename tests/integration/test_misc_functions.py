import random

import pytest
from normalizer import mapper
from pyspark.sql import functions as F

from mutation_indexer.driver.builders import utils
from tests.integration import config
from tests.integration.utils import true_stats

conf = config.TestConfig()


def create_df(sqlContext, values, column_name="values"):
    """
    Creates 1d mock dataframe from values list and column name
    """
    values = map(lambda v: (v,), values)
    return sqlContext.createDataFrame(values, [column_name])


@pytest.mark.parametrize(
    "index",
    ["ssm_centric", "ssm_occurrence_centric", "cnv_centric", "cnv_occurrence_centric"],
)
def test_struct_select_without_selector(sqlContext, index):
    model_mapper = mapper.ModelMapper(index)

    # Make sure that we are actually testing something
    assert len(model_mapper.nested_mappings) > 0

    for mapping in model_mapper.nested_mappings:
        stmt = utils.struct_select(index, mapping)
        assert stmt


@pytest.mark.parametrize(
    "index, selector",
    [
        ("case_centric", lambda x: x[:1]),
        ("case_centric", lambda x: [x[1]] if len(x) > 1 else x[:1]),
        ("gene_centric", lambda x: x[:1]),
        ("gene_centric", lambda x: [x[1]] if len(x) > 1 else x[:1]),
    ],
)
def test_struct_select_with_selector(sqlContext, index, selector):
    """
    Since case_centric and gene_centric have same nested mappings under
    different paths we need to provide a selector to resolve it.
    """
    model_mapper = mapper.ModelMapper(index)

    # Make sure that we are actually testing something
    assert len(model_mapper.nested_mappings) > 0

    for mapping in model_mapper.nested_mappings:
        stmt = utils.struct_select(index, mapping, selector=selector)
        assert stmt


@pytest.mark.usefixtures("sqlContext", "maf_df", "gistic_df", "source_es_client")
class TestMiscFunctions:
    def test_es_adapter(self, sqlContext):
        """
        Test that the elasticsearch-hadoop wrapper jar is loaded
        """
        # Fails if org.elasticsearch.hadoop.mr.LinkedMapWritable isnt in the path
        return (
            sqlContext.read.format("es")
            .option("es.nodes", conf.source_es_nodes)
            .option("es.nodes.resolve.hostname", "false")
            .option("es.resource.read", conf.graph_case_index)
            .load(conf.graph_case_index)
        )

    def test_percentile(self):
        """
        Test the percentile util function
        """
        length = random.randint(0, 100)
        if length % 2:
            length += 1
        v = [random.randint(0, 100) for i in range(length + 1)]
        sorted_v = sorted(v)
        assert utils.percentile(v, 0) == sorted_v[0]
        assert utils.percentile(v, 50) == sorted_v[length // 2]
        assert utils.percentile(v, 100) == sorted_v[-1]

    def test_graph_index(self, source_es_client):
        """
        Test the test graph index fixtures
        """
        assert source_es_client is not None
        assert conf.graph_case_index is not None
        assert conf.graph_file_index is not None
        assert source_es_client.count()["count"] > 0

        def assert_existence(index_name, doc_id):
            doc = source_es_client.get(index=index_name, id=doc_id)
            assert doc is not None, "Could not find {} in {}".format(doc_id, index_name)
            assert doc["_id"] == doc_id

        assert_existence(conf.graph_case_index, "d2748e35-4719-43c1-a533-b6b0cd9688c3")
        assert_existence(conf.graph_file_index, "2a8f2c83-8b5e-4987-8dbf-01f7ee24dc26")

    def test_properties(self):
        """
        Test that configuration properties are present
        """
        assert "s3_host" in dir(conf)
        assert "es_nodes" in dir(conf)

    def test_sanitize_aa_change(self, sqlContext):
        # Fake input and expected output
        fake_input = ["a", "p.b", "cp."]
        expected_output = ["a", "b", "c"]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, "aa_change")
        expected_df = create_df(sqlContext, expected_output, "aa_change")

        # Test:
        assert utils.sanitize_aa_change(df).collect() == expected_df.collect()

    def test_extract_impact_or_score(self, sqlContext):
        # Fake input and expected output
        fake_input = [
            "possibly_damaging(0.475)",
            "deleterious_low_confidence(0)",
            "zero_decimal(0.)",
            "",
        ]
        expected_impact_output = [
            "possibly_damaging",
            "deleterious_low_confidence",
            "zero_decimal",
            "",
        ]
        expected_score_output = [0.475, 0.0, 0.0, None]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, "field")
        expected_impact_df = create_df(
            sqlContext, expected_impact_output, "field_impact"
        )
        expected_score_df = create_df(sqlContext, expected_score_output, "field_score")

        # Test:
        df = utils.extract_impact(df, "field", "field_impact")
        df = utils.extract_score(df, "field", "field_score")

        assert df.select("field_impact").collect() == expected_impact_df.collect()
        assert df.select("field_score").collect() == expected_score_df.collect()

    def test_sanitize_gene_aa_change(self, sqlContext):
        # Fake input and expected output
        fake_input = [["c", "a", "a", "", None, "b", "c", "c"]]
        expected_output = [["a", "b", "c"]]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, "gene_aa_change")
        expected_df = create_df(sqlContext, expected_output, "gene_aa_change")

        # Test:
        assert utils.sanitize_gene_aa_change(df).collect() == expected_df.collect()

    def test_convert_empty_str_to_null_in_col(self, sqlContext):
        # Fake input and expected output
        fake_input = ["a", "", "c", ""]
        expected_output = ["a", None, "c", None]

        # Convert to dataframes:
        df = create_df(sqlContext, fake_input, "test")
        expected_df = create_df(sqlContext, expected_output, "test")

        # Test:
        assert (
            utils.convert_empty_str_to_null_in_col(df, "test").collect()
            == expected_df.collect()
        )

    def test_aa_start_end(self, maf_df):
        """
        Test aa_start and aa_end extraction
        """
        new_df = maf_df.withColumn("aa_change", F.lit("p.L1201R"))
        new_df = utils.extract_aas_position(new_df)

        assert "aa_start" in new_df.columns
        assert "aa_end" in new_df.columns
        aa_change = (
            new_df.filter(new_df.aa_change == "p.L1201R")
            .select("aa_start", "aa_end")
            .collect()[0]
        )
        assert aa_change["aa_start"] == 1201
        assert aa_change["aa_end"] == 1201

    def test_ssm_label(self):
        """
        Test ssm label generation
        """
        label = utils.ssm_label("chr3", "SNP", 41589825, "", "A", "T")
        assert label == "chr3:g.41589825A>T"

        label = utils.ssm_label("chr3", "DEL", 41589825, "", "A", "")
        assert label == "chr3:g.41589825delA"

        # TODO Other indel cases
        label = utils.ssm_label("chr3", "INS", 41589825, 41589825, "", "T")
        assert label == "chr3:g.41589825_41589825insT"

        label = utils.ssm_label("chr4", "SNP", 112382545, "", "A", "T")
        assert label == "chr4:g.112382545A>T"

        label = utils.ssm_label("chr5", "DNP", 112382500, 112382501, "AC", "TG")
        assert label == "chr5:g.112382500_112382501delinsTG"

        label = utils.ssm_label("chr5", "TNP", 112382500, 112382502, "ACT", "TGA")
        assert label == "chr5:g.112382500_112382502delinsTGA"

        label = utils.ssm_label("chr5", "ONP", 112382500, 112382505, "TCGATC", "CTAGCT")
        assert label == "chr5:g.112382500_112382505delinsCTAGCT"

    def test_uuid5(self):
        """
        Test uuid5 generation
        """

        ssm_id = utils.generate_uuid5(
            "ssm", "GRCh38", "chr4", "112382545", "112382545", "SNP", "A", "T"
        )
        assert ssm_id == "3439eab1-0c63-50cd-bad7-1ae8ffa8aa01"

        ssm_occ_id = utils.generate_uuid5(
            "ssm_occurrence",
            "642a6e7d-8b15-5f93-9e29-22c9649e9058",
            "13afbde8-e5b5-4f3c-8a9d-daef71560005",
        )
        assert ssm_occ_id == "f4222c55-fea2-5b23-a204-482f33492800"

    @pytest.mark.parametrize("index", conf.indices)
    def test_test_data_stats(self, maf_df, gistic_df, index):
        """
        Test that TestDataStats loads test data and returns stats
        """
        data = true_stats.TestDataStats.load_test_data(conf.input_dir)
        stats = true_stats.TestDataStats.get_stats(maf_df, gistic_df, data, index)

        if index in ["gene_expression"]:
            assert stats is None
        else:
            assert stats is not None
