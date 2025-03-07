import logging
import pathlib
import tempfile
from collections.abc import Iterable, Iterator
from typing import Any
from unittest import mock

import elasticsearch
import pytest
from elasticsearch import helpers
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration, es_utils, indexd_utils
from mutation_indexer.builders import gene_expression
from mutation_indexer.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ge_config() -> Iterator[configuration.Configuration]:
    with tempfile.TemporaryDirectory() as tmpdir:

        def pre_load(data: dict) -> dict:
            data["build"]["index_types"] = ["GENE_EXPRESSION"]

            backup = data["builders"]["gene_expression"]["gene_expression"]["backup"]
            backup["path"] = tmpdir + "/" + backup["path"]

            return data

        yield test_setup.load_configuration(pre_load)


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
        es_utils.SchemaLoader(),
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

    def mock_get(file_id: str) -> client.Document | None:
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
def expression_value_df(
    ge_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    indexd: client.IndexClient,
    gene_model_df: sql.DataFrame,
    primary_aliquot_df: sql.DataFrame,
) -> sql.DataFrame:
    return gene_expression.ExpressionValueBuilder(
        ge_config.builders.gene_expression.expression_value,
        spark_session,
        indexd_utils.DataFrameUtil(indexd, spark_session, mock.MagicMock()),
    ).build(gene_model_df=gene_model_df, primary_aliquot_df=primary_aliquot_df)


@pytest.fixture
def ge_builder(
    ge_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> Iterable[gene_expression.IndexBuilder]:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        ge_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )

    yield gene_expression.IndexBuilder(
        ge_config.builders.gene_expression.gene_expression,
        spark_session,
        es_dataframe_util,
        mappings_loader,
    )

    # Delete the index
    ge_index = ge_config.elasticsearch.write.indices[build.IndexType.GENE_EXPRESSION]
    es_client.indices.delete(index=ge_index, ignore_unavailable=True)


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
    expression_value_df: sql.DataFrame,
) -> None:
    ge_index = ge_config.elasticsearch.write.indices[build.IndexType.GENE_EXPRESSION]

    ge_builder.build(expression_value_df=expression_value_df)

    es_client.indices.refresh()

    hits = helpers.scan(es_client, index=ge_index)
    expressions = tuple(h["_source"] for h in hits)
    gene_ids = frozenset(e["gene_id"] for e in expressions)

    assert len(expressions) == 50
    assert len(gene_ids) == 10


@pytest.mark.usefixtures("ge_file_docs")
def test_gene_expression_builder_writes_backup_to_path(
    ge_config: configuration.Configuration,
    ge_builder: gene_expression.IndexBuilder,
    expression_value_df: sql.DataFrame,
    spark_session: sql.SparkSession,
) -> None:
    ge_config.elasticsearch.write.indices[build.IndexType.GENE_EXPRESSION]
    # Assert default congfiguration (ideally, we should modify Configuration here but it's a frozen dataclass).
    assert ge_config.build.build_version == "v0"
    assert ge_config.build.data_release == "test"
    assert (
        ge_config.builders.gene_expression.gene_expression.backup.mode
        == build.BackupMode.WRITE
    )
    assert ge_config.builders.gene_expression.gene_expression.backup.path.endswith(
        "./data_release/test/v0/gene_expression_test_v0.parquet"
    )
    assert (
        ge_config.builders.gene_expression.gene_expression.backup.partition_by
        == "gene_id"
    )

    ge_builder.build(expression_value_df=expression_value_df)

    parquet_dump = pathlib.Path(
        ge_config.builders.gene_expression.gene_expression.backup.path
    )
    assert parquet_dump.exists()
    assert parquet_dump.is_dir()

    df = spark_session.read.parquet(str(parquet_dump))
    type_dict = dict(df.dtypes)
    assert type_dict["log2_uqfpkm"] == "float"
    assert type_dict["uqfpkm"] == "float"
