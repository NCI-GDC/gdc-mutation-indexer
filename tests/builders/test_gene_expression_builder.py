import json
import os

import ndjson
from indexclient.client import IndexClient, Document
import pytest
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
from pyspark.sql.functions import col

from exports.builders.gene_expression import (
    GeneExpressionBuilder,
    GeneExpressionCaseInputBuilder,
    GeneExpressionValueInputBuilder,
    trim_gene_id,
)
from tests_config import TestConfig


@pytest.fixture(scope="module")
def ge_conf():
    conf = TestConfig()
    conf.index_types = ["gene_expression"]
    conf.indices = conf.get_index_names()

    return conf


@pytest.fixture
def ge_builder(sqlContext, ge_conf):
    builder = GeneExpressionBuilder(ge_conf, sqlContext)

    return builder


@pytest.fixture
def ge_cases_df(sqlContext, ge_conf):
    cases_df = GeneExpressionCaseInputBuilder(
        ge_conf,
        sqlContext,
        "gene_expression_cases",
    ).build()

    assert cases_df.schema == StructType([
        StructField("case_id", StringType(), True),
        StructField("days_to_death", LongType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("race", StringType(), True),
        StructField("vital_status", StringType(), True),
        StructField("submitter_id", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("file_url", StringType(), True),
        StructField("age_at_diagnosis", ArrayType(LongType(), True), True),
    ])

    return cases_df

@pytest.fixture
def ge_values_df(sqlContext, ge_conf):
    return GeneExpressionValueInputBuilder(
        ge_conf,
        sqlContext,
        "gene_expression_values",
    ).build()


@pytest.fixture
def mock_indexd_requests(monkeypatch, ge_conf):
    path = os.path.join(ge_conf.input_dir, "ge")

    existing_files = os.listdir(path)

    def make_document(file_id, filename):
        url = "file://" + os.path.join(path, filename)
        urls = [url]
        urls_metadata = {url: {"type": "cleversafe", "state": "validated"}}
        return Document(
            client=None,
            did=file_id,
            json={"urls": urls, "urls_metadata": urls_metadata},
        )

    def mock_get(_, file_id):
        filename = file_id + ".txt"
        if filename not in existing_files:
            return None

        return make_document(file_id, filename)

    def mock_bulk_request(_, dids):
        results = []
        for file_id in dids:
            filename = file_id + ".txt"

            if filename not in existing_files:
                continue

            results.append(make_document(file_id, filename))
        return results

    monkeypatch.setattr(IndexClient, "get", mock_get)
    monkeypatch.setattr(IndexClient, "bulk_request", mock_bulk_request)


@pytest.fixture
def ge_file_docs(source_es_client, ge_conf):
    path = os.path.join(ge_conf.input_dir, "ge-files.ndjson")
    with open(path) as f:
        docs = ndjson.load(f)

    for doc in docs:
        source_es_client.index(
            index=ge_conf.graph_file_index,
            doc_type=ge_conf.graph_file_doc_type,
            id=doc["file_id"],
            body=doc,
        )

    source_es_client.indices.refresh(ge_conf.graph_file_index)

    yield docs

    for doc in docs:
        source_es_client.delete(
            index=ge_conf.graph_file_index,
            doc_type=ge_conf.graph_file_doc_type,
            id=doc["file_id"],
        )


def test_trim_gene_version(sqlContext):
    df = sqlContext.createDataFrame(
        [{"raw_gene_id": "ENS001.1"}, {"raw_gene_id": "ENS002.2"}]
    )

    new_df = df.withColumn("gene_id", trim_gene_id(col("raw_gene_id")))

    assert {row["gene_id"] for row in new_df.collect()} == {"ENS001", "ENS002"}


@pytest.mark.usefixtures("ge_file_docs", "mock_indexd_requests")
def test_gene_expression_builder(ge_builder, es_client, ge_conf, ge_cases_df, ge_values_df):
    ge_builder.build(ge_cases_df, ge_values_df).load()

    es_client.indices.refresh()

    response = es_client.search(index=ge_conf.indices["gene_expression"])
    hits = response["hits"]

    assert hits["total"] == {"relation": "eq", "value": 5}

    # Make sure that "genes" field is excluded from source
    for hit in hits["hits"]:
        assert "genes" not in hit

    # Make sure that correct values have been indexed
    aggs = {
        "genes": {
            "nested": {"path": "genes"},
            "aggs": {
                "gene_counts": {"terms": {"field": "genes.gene_id", "size": 200}},
            }
        }
    }

    agg_response = es_client.search(index=ge_conf.indices["gene_expression"],
                                    body={"size": 0, "aggs": aggs})
    gene_counts = agg_response["aggregations"]["genes"]

    # Make sure that only protein_coding genes have been selected (mock data contains only 10)
    assert gene_counts["doc_count"] == 50
    assert len(gene_counts["gene_counts"]["buckets"]) == 10
