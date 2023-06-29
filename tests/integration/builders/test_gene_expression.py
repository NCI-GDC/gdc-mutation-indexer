import logging
import pathlib
from collections import Iterable, Iterator
from typing import Any, Optional
from unittest import mock

import elasticsearch
import pytest
from indexclient import client
from pyspark import sql
from pyspark.sql import types

from exports import builders, configuration, es_utils, indexd_utils
from exports.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ge_config() -> configuration.Configuration:
    def pre_load(data: dict) -> dict:
        data["build"]["index_types"] = ["GENE_EXPRESSION"]

        return data

    return test_setup.load_configuraiton(pre_load)


@pytest.fixture(scope="module")
def primary_aliquot_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    ge_file_docs: Any,
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        es_client,
        es_utils.MappingsLoader(),
    )
    primary_aliquot_builder = builders.GeneExpressionPrimaryAliquotBuilder(
        default_config.builders.gene_expression.primary_aliquot,
        spark_session,
        es_dataframe_util,
    )

    return primary_aliquot_builder.build()


@pytest.fixture
def ge_builder(
    ge_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> builders.GeneExpressionBuilder:
    mappings_loader = es_utils.MappingsLoader()
    dataframe_util = es_utils.DataFrameUtil(
        ge_config.elasticsearch, spark_session, es_client, mappings_loader
    )

    return builders.GeneExpressionBuilder(
        ge_config.builders.gene_expression.gene_expression,
        spark_session,
        dataframe_util,
        mappings_loader,
    )


@pytest.fixture(scope="module")
def case_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    primary_aliquot_df: sql.DataFrame,
) -> sql.DataFrame:
    cases_df = builders.GeneExpressionCaseInputBuilder(
        default_config.builders.gene_expression.case,
        spark_session,
    ).build(primary_aliquot_df=primary_aliquot_df)

    assert cases_df.schema == types.StructType(
        [
            types.StructField(
                "age_at_diagnosis",
                types.ArrayType(types.LongType(), True),  # type: ignore
                True,
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


@pytest.fixture(scope="function")
def expression_value_df(
    default_config: configuration.Configuration,
    sqlContext: sql.SQLContext,
    spark_session: sql.SparkSession,
    indexd: client.IndexClient,
    primary_aliquot_df: sql.DataFrame,
) -> sql.DataFrame:
    gene_model_df = builders.GeneModelBuilder(
        default_config.builders.gene_expression.gene_model, spark_session
    ).build()
    doc_dataframe_util = indexd_utils.DataFrameUtil(
        indexd, sqlContext, mock.MagicMock()
    )

    return builders.GeneExpressionValueInputBuilder(
        default_config.builders.gene_expression.expression_value,
        spark_session,
        doc_dataframe_util,
    ).build(
        gene_model_df=gene_model_df,
        primary_aliquot_df=primary_aliquot_df,
    )


@pytest.fixture(scope="function")
def indexd(input_dir: pathlib.Path) -> client.IndexClient:
    path = input_dir / "ge"
    existing_files = frozenset(p.name for p in path.glob("**/*"))

    def make_document(file_id: str, filename: str) -> client.Document:
        url = "file://" + str(path / filename)
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

    def mock_bulk_request(dids: Iterable[str]) -> Iterable[client.Document]:
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
def ge_file_docs(
    ge_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_graph_indices: Any,
) -> Iterator[Any]:
    ge_data_file = input_dir / "ge-files.ndjson"

    with test_setup.IndexManager(
        ge_config,
        es_client,
        logger,
        index_types=(build.IndexType.GENE_EXPRESSION,),
        skip_creation=True,
    ):
        with test_setup.DocumentLoader(ge_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.FILE, ge_data_file)


@pytest.mark.usefixtures("ge_file_docs")
def test_gene_expression_builder(
    ge_config: configuration.Configuration,
    ge_builder: builders.GeneExpressionBuilder,
    es_client: elasticsearch.Elasticsearch,
    case_df: sql.DataFrame,
    expression_value_df: sql.DataFrame,
) -> None:
    ge_index = ge_config.elasticsearch.write.indices[build.IndexType.GENE_EXPRESSION]
    inputs = {"case_df": case_df, "expression_value_df": expression_value_df}

    ge_builder.build(**inputs)

    es_client.indices.refresh()

    response = es_client.search(index=ge_index)
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

    agg_response = es_client.search(index=ge_index, body={"size": 0, "aggs": aggs})
    gene_counts = agg_response["aggregations"]["genes"]

    # Make sure that only protein_coding genes have been selected (mock data contains only 10)
    assert gene_counts["doc_count"] == 50
    assert len(gene_counts["gene_counts"]["buckets"]) == 10
