import logging
import pathlib
from typing import Any, Callable, Iterable, Iterator, Optional
from unittest import mock

import elasticsearch
import pytest
from indexclient import client
from pyspark import sql
from pyspark.sql import types

import config
from exports import builders, configuration, es_utils, indexd_utils
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ge_file_docs(
    es_client: elasticsearch.Elasticsearch,
    default_config: configuration.Configuration,
    input_dir: pathlib.Path,
    setup_graph_indices: Any,
) -> Iterator[Any]:
    path = input_dir.joinpath("ge-files.ndjson")

    with test_setup.DocumentLoader(default_config, es_client, logger) as loader:
        yield loader.load_docs("file", str(path))


@pytest.fixture(scope="module")
def mock_indexd(input_dir: pathlib.Path, ge_file_docs: Any) -> None:
    path = input_dir.joinpath("ge")
    existing_files = frozenset(f.name for f in path.glob("*"))

    def make_document(file_id: str, filename: str) -> client.Document:
        url = f"file://{path.joinpath(filename)}"
        urls = [url]
        urls_metadata = {url: {"type": "cleversafe", "state": "validated"}}
        return client.Document(
            client=None,
            did=file_id,
            json={"urls": urls, "urls_metadata": urls_metadata},
        )

    def mock_get(file_id: str) -> Optional[client.Document]:
        filename = file_id + ".txt"
        if filename not in existing_files:
            return None

        return make_document(file_id, filename)

    def mock_bulk_request(dids: Iterable[str]) -> Iterable[Optional[client.Document]]:
        results = []
        for file_id in dids:
            filename = file_id + ".txt"

            if filename not in existing_files:
                continue

            results.append(make_document(file_id, filename))
        return results

    indexd = mock.MagicMock(spec=client.IndexClient)
    indexd.get.side_effect = mock_get
    indexd.bulk_request.side_effect = mock_bulk_request

    return indexd


@pytest.fixture(scope="module")
def ge_conf(
    configure_gene_model: Callable[[dict], dict],
    es_client: elasticsearch.Elasticsearch,
    mock_indexd: client.IndexClient,
) -> config.BaseConfig:
    def add_ge_to_build(data: dict) -> dict:
        data["build"]["index_types"] = ["GENE_EXPRESSION"]

        return data

    conf = test_setup.load_configuraiton(configure_gene_model, add_ge_to_build)

    return config.ConfigAdapter(conf, es_client, mock_indexd)


@pytest.fixture(scope="module")
def ge_primary_aliquot_df(
    ge_conf: config.BaseConfig,
    sqlContext: sql.SQLContext,
    es_client: elasticsearch.Elasticsearch,
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(ge_conf, sqlContext, es_client)
    primary_aliquot_builder = builders.GeneExpressionPrimaryAliquotBuilder(
        ge_conf, sqlContext, es_dataframe_util
    )

    return primary_aliquot_builder.build()


@pytest.fixture
def ge_builder(sqlContext: sql.SQLContext, ge_conf: config.BaseConfig):
    builder = builders.GeneExpressionBuilder(ge_conf, sqlContext)

    return builder


@pytest.fixture(scope="module")
def ge_cases_df(
    sqlContext: sql.SQLContext,
    ge_conf: config.BaseConfig,
    ge_primary_aliquot_df: sql.DataFrame,
) -> sql.DataFrame:
    cases_df = builders.GeneExpressionCaseInputBuilder(
        ge_conf,
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
def ge_values_df(
    sqlContext: sql.SQLContext,
    ge_conf: config.BaseConfig,
    ge_primary_aliquot_df: sql.DataFrame,
) -> sql.DataFrame:
    gene_model_df = builders.GeneModelBuilder(ge_conf, sqlContext).build()
    doc_dataframe_util = indexd_utils.DataFrameUtil(
        ge_conf.indexd, sqlContext, mock.MagicMock()
    )

    return builders.GeneExpressionValueInputBuilder(
        ge_conf, sqlContext, doc_dataframe_util
    ).build(
        gene_model_df=gene_model_df,
        gene_expression_primary_aliquot_df=ge_primary_aliquot_df,
    )


def test_gene_expression_builder(
    ge_builder: builders.GeneExpressionBuilder,
    es_client: elasticsearch.Elasticsearch,
    ge_conf: config.BaseConfig,
    ge_cases_df: sql.DataFrame,
    ge_values_df: sql.DataFrame,
) -> None:
    try:
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

    finally:
        es_client.indices.delete(
            index=ge_conf.indices["gene_expression"], ignore_unavailable=True
        )
