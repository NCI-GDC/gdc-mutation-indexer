import os
from unittest import mock

import ndjson
import pytest
from indexclient import client
from pyspark import sql
from pyspark.sql import types
from exports.configuration.builders import common, gene_expression
from exports.constants import build

from tests.integration import config
from exports import builders, es_utils, indexd_utils


@pytest.fixture(scope="module")
def ge_conf():
    conf = config.TestConfig()
    conf.index_types = ["gene_expression"]
    conf.indices = conf.get_index_names()

    return conf


@pytest.fixture(scope="module")
def ge_primary_aliquot_df(
    ge_conf: config.BaseConfig, sqlContext: sql.SQLContext
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(ge_conf, sqlContext)
    config = gene_expression.Builder(
            is_cached=True,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            projects=(),
        )
    primary_aliquot_builder = builders.GeneExpressionPrimaryAliquotBuilder(
        ge_conf, sqlContext, es_dataframe_util
    )

    return primary_aliquot_builder.build()


@pytest.fixture
def ge_builder(sqlContext, ge_conf):
    builder = builders.GeneExpressionBuilder(ge_conf, sqlContext)

    return builder


@pytest.fixture(scope="module")
def ge_cases_df(sqlContext, ge_primary_aliquot_df):
    config = gene_expression.Builder(
        is_cached=True,
        backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
        projects=(),
    )
    cases_df = builders.GeneExpressionCaseInputBuilder(
        config,
        sqlContext,
    ).build(gene_expression_primary_aliquot_df=ge_primary_aliquot_df)

    assert cases_df.schema == types.StructType(
        [
            types.StructField(
                "age_at_diagnosis", types.ArrayType(types.LongType(), True), True
            ),
            types.StructField("case_id", types.StringType(), True),
            types.StructField("days_to_death", types.LongType(), True),
            types.StructField("ethnicity", types.StringType(), True),
            types.StructField("file_id", types.StringType(), True),
            types.StructField("gender", types.StringType(), True),
            types.StructField("project_id", types.StringType(), True),
            types.StructField("race", types.StringType(), True),
            types.StructField("submitter_id", types.StringType(), True),
            types.StructField("vital_status", types.StringType(), True),
        ]
    )

    return cases_df


@pytest.fixture
def ge_values_df(sqlContext, ge_conf, ge_primary_aliquot_df):
    gene_model_df = builders.GeneModelBuilder(ge_conf, sqlContext).build()
    doc_dataframe_util = indexd_utils.DataFrameUtil(
        ge_conf.indexd, sqlContext, mock.MagicMock()
    )
    config = gene_expression.Builder(
        is_cached=True,
        backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
        projects=(),
    )

    return builders.GeneExpressionValueInputBuilder(
        config, sqlContext, doc_dataframe_util
    ).build(
        gene_model_df=gene_model_df,
        gene_expression_primary_aliquot_df=ge_primary_aliquot_df,
    )


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
