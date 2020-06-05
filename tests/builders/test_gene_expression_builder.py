import json
import os

from indexclient.client import IndexClient, Document
import pytest

from exports.builders.gene_expression import (
    GeneExpressionBuilder,
    ExpressionCountsBuilder,
)
from tests_config import TestConfig

conf = TestConfig()


@pytest.fixture
def ge_builder(sqlContext):
    builder = GeneExpressionBuilder(conf, sqlContext)

    return builder


@pytest.fixture
def expression_counts_builder(sqlContext):
    builder = ExpressionCountsBuilder(conf, sqlContext, "gene_expression")

    return builder


@pytest.fixture
def mock_indexd_get(monkeypatch):
    path = os.path.join(conf.input_dir, "ge")

    existing_files = os.listdir(path)

    def mock_get(_, file_id):
        filename = file_id + ".txt"
        if filename not in existing_files:
            return None

        url = "file://" + os.path.join(path, filename)
        urls = [url]
        urls_metadata = {url: {"type": "cleversafe", "state": "validated"}}
        return Document(
            client=None,
            did=file_id,
            json={"urls": urls, "urls_metadata": urls_metadata},
        )

    monkeypatch.setattr(IndexClient, "get", mock_get)


@pytest.fixture
def ge_file_docs(source_es_client):
    path = os.path.join(conf.input_dir, "ge-files.json")
    with open(path) as f:
        docs = json.load(f)

    for doc in docs:
        source_es_client.index(
            index=conf.graph_file_index,
            doc_type=conf.graph_file_doc_type,
            id=doc["file_id"],
            body=doc,
        )

    source_es_client.indices.refresh(conf.graph_file_index)

    yield docs

    for doc in docs:
        source_es_client.delete(
            index=conf.graph_file_index,
            doc_type=conf.graph_file_doc_type,
            id=doc["file_id"],
        )


@pytest.mark.usefixtures("ge_file_docs", "mock_indexd_get")
def test_gene_experssion_builder(ge_builder, es_client, expression_counts_builder):
    counts_df = expression_counts_builder.build_from_scratch()
    ge_builder.build(counts_df).load()

    es_client.indices.refresh()

    response = es_client.search(index=conf.indices["gene_expression"])
    hits = response["hits"]

    assert hits["total"] == {"relation": "eq", "value": 5}
