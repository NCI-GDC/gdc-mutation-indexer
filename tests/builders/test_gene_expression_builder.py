import os

import pytest
from indexclient import client

import ndjson
import tests_config
from exports import builders
from exports.builders import gene_expression
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql import types


@pytest.fixture(scope="module")
def ge_conf():
    conf = tests_config.TestConfig()
    conf.index_types = ["gene_expression"]
    conf.indices = conf.get_index_names()

    return conf


@pytest.fixture
def ge_builder(sqlContext, ge_conf):
    builder = builders.GeneExpressionBuilder(ge_conf, sqlContext)

    return builder


@pytest.fixture
def ge_cases_df(sqlContext, ge_conf):
    cases_df = builders.GeneExpressionCaseInputBuilder(
        ge_conf,
        sqlContext,
        "gene_expression_cases",
    ).build()

    assert cases_df.schema == types.StructType(
        [
            types.StructField("case_id", types.StringType(), True),
            types.StructField("days_to_death", types.LongType(), True),
            types.StructField("ethnicity", types.StringType(), True),
            types.StructField("gender", types.StringType(), True),
            types.StructField("race", types.StringType(), True),
            types.StructField("vital_status", types.StringType(), True),
            types.StructField("submitter_id", types.StringType(), True),
            types.StructField("project_id", types.StringType(), True),
            types.StructField("file_url", types.StringType(), True),
            types.StructField(
                "age_at_diagnosis", types.ArrayType(types.LongType(), True), True
            ),
        ]
    )

    return cases_df


@pytest.fixture
def ge_values_df(sqlContext, ge_conf):
    return builders.GeneExpressionValueInputBuilder(
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
        return client.Document(
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

    monkeypatch.setattr(client.IndexClient, "get", mock_get)
    monkeypatch.setattr(client.IndexClient, "bulk_request", mock_bulk_request)


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
        [sql.Row(raw_gene_id="ENS001.1"), sql.Row(raw_gene_id="ENS002.2")]
    )

    new_df = df.withColumn(
        "gene_id", gene_expression.trim_gene_id(f.col("raw_gene_id"))
    )

    assert {row["gene_id"] for row in new_df.collect()} == {"ENS001", "ENS002"}


@pytest.mark.usefixtures("ge_file_docs", "mock_indexd_requests")
def test_gene_expression_builder(
    ge_builder, es_client, ge_conf, ge_cases_df, ge_values_df
):
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
            },
        }
    }

    agg_response = es_client.search(
        index=ge_conf.indices["gene_expression"], body={"size": 0, "aggs": aggs}
    )
    gene_counts = agg_response["aggregations"]["genes"]

    # Make sure that only protein_coding genes have been selected (mock data contains only 10)
    assert gene_counts["doc_count"] == 50
    assert len(gene_counts["gene_counts"]["buckets"]) == 10
