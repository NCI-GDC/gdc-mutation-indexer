import logging
import pathlib
from collections import Iterable, Iterator
from typing import Any, Optional
from unittest import mock

import elasticsearch
from elasticsearch import helpers
import pytest
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration, es_utils, indexd_utils
from mutation_indexer.builders import gene_expression
from mutation_indexer.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ge_config() -> configuration.Configuration:
    def pre_load(data: dict) -> dict:
        data["build"]["index_types"] = ["GENE_EXPRESSION"]
        # TODO: remove this (only for debugging purposes)
        # From configuration.toml:
        # [builders.gene_expression.case.backup]
        # mode = "NEITHER"
        # path = ""
        backup = data["builders"]["gene_expression"]["case"]["backup"] = {}
        backup["mode"] = build.BackupMode.WRITE.name
        path = pathlib.Path().cwd() / "cases.parquet"
        backup["path"] = str(path.absolute())

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
    primary_aliquot_builder = gene_expression.PrimaryAliquotBuilder(
        default_config.builders.gene_expression.primary_aliquot,
        spark_session,
        es_dataframe_util,
    )

    return primary_aliquot_builder.build()


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


@pytest.fixture
def ge_builder(
    ge_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    indexd: client.IndexClient,
) -> gene_expression.IndexBuilder:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        ge_config.elasticsearch, spark_session, es_client, mappings_loader
    )
    doc_dataframe_util = indexd_utils.DataFrameUtil(
        indexd, spark_session, logger=mock.MagicMock()
    )

    return gene_expression.IndexBuilder(
        ge_config.builders.gene_expression.gene_expression,
        spark_session,
        es_dataframe_util,
        mappings_loader,
        doc_dataframe_util,
    )


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
    ge_builder: gene_expression.IndexBuilder,
    es_client: elasticsearch.Elasticsearch,
    gene_model_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
) -> None:
    ge_index = ge_config.elasticsearch.write.indices[build.IndexType.GENE_EXPRESSION]
    inputs = gene_expression.IndexBuilderInputs(
        gene_model_df=gene_model_df, primary_aliquot_df=primary_aliquot_df
    )

    ge_builder.build(**inputs)

    es_client.indices.refresh()

    hits = helpers.scan(es_client, index=ge_index)
    expressions = tuple(h["_source"] for h in hits)
    gene_ids = frozenset(e["gene_id"] for e in expressions)

    assert len(expressions) == 50
    assert len(gene_ids) == 10
