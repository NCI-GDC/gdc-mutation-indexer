"""Fixtures shared between segment_cnv_centric and segment_cnv_occurrence_centric."""

import logging
import pathlib
from collections.abc import Iterable, Iterator, Set
from typing import Any, Optional
from unittest import mock

import elasticsearch
import pytest
from indexclient import client
from pyspark import sql

from mutation_indexer import es_utils, indexd_utils
from mutation_indexer.builders import (
    ascat_metadata,
    case,
    segment_cnv,
    segment_cnv_metadata,
)
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def segment_config() -> configuration.Configuration:
    return test_setup.load_viz_config({"build": {"acl": ["open"]}})


@pytest.fixture(scope="package")
def setup_elasticsearch_indices(
    es_client: elasticsearch.Elasticsearch, segment_config: configuration.Configuration
) -> Iterator[Any]:
    mappings_loader = es_utils.MappingsLoader()
    file_index = segment_config.elasticsearch.read.file_index
    case_index = segment_config.elasticsearch.read.case_index
    file_index_exists = es_client.indices.exists(index=file_index)
    case_index_exists = es_client.indices.exists(index=case_index)
    if file_index_exists is False:
        model_mapper = mappings_loader.load_mapper(build.IndexType.FILE)
        es_client.indices.create(
            index=file_index,
            settings=model_mapper.settings,
            mappings=model_mapper.mappings,
        )
    if case_index_exists is False:
        model_mapper = mappings_loader.load_mapper(build.IndexType.CASE)
        es_client.indices.create(
            index=case_index,
            settings=model_mapper.settings,
            mappings=model_mapper.mappings,
        )

    segment_cnv_indices_manager = test_setup.IndexManager(
        segment_config,
        es_client,
        logger,
        (
            build.IndexType.SEGMENT_CNV_CENTRIC,
            build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
        ),
        skip_creation=True,
    )
    with segment_cnv_indices_manager:
        yield
        if file_index_exists is False:
            es_client.indices.delete(index=file_index, ignore_unavailable=True)
        if case_index_exists is False:
            es_client.indices.delete(index=case_index, ignore_unavailable=True)


@pytest.fixture(scope="package")
def indexd(input_dir: pathlib.Path) -> client.IndexClient:
    """Mocks IndexClient get() and bulk_request() methods."""
    path = input_dir / "segment_cnv" / "file_data"
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
    indexd.get = mock_get
    indexd.bulk_request = mock_bulk_request

    return indexd


@pytest.fixture(scope="package")
def segment_file_docs(
    segment_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_elasticsearch_indices: Any,
) -> Iterator[Set[str]]:
    segment_file = input_dir / "segment_cnv" / "segment_cnv-files.ndjson"

    with test_setup.IndexManager(
        segment_config,
        es_client,
        logger,
        index_types=(build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,),
        skip_creation=True,
    ):
        with test_setup.DocumentLoader(segment_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.FILE, segment_file)


@pytest.fixture(scope="package")
def ascat_file_docs(
    segment_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_elasticsearch_indices: Any,
) -> Iterator[Set[str]]:
    ascat_file = input_dir / "segment_cnv" / "ascat_metadata-files.ndjson"

    with test_setup.DocumentLoader(segment_config, es_client, logger) as loader:
        yield loader.load_docs(build.IndexType.FILE, ascat_file)


@pytest.fixture(scope="package")
def ascat_metadata_df(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    ascat_file_docs: Set[str],
) -> sql.DataFrame:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        segment_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    es_rdd_util = es_utils.RDDUtil(
        segment_config.elasticsearch, spark_session.sparkContext
    )
    df = ascat_metadata.ASCATMetadataBuilder(
        segment_config.builders.ascat_metadata,
        spark_session,
        es_dataframe_util,
        es_rdd_util,
    ).build()

    return df


@pytest.fixture(scope="package")
def segment_case_docs(
    segment_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_elasticsearch_indices: Any,
) -> Iterator[Set[str]]:
    segment_case = input_dir / "segment_cnv" / "segment_cnv-cases.ndjson"

    with test_setup.DocumentLoader(segment_config, es_client, logger) as loader:
        yield loader.load_docs(build.IndexType.CASE, segment_case)


@pytest.fixture(scope="package")
def case_df(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    maf_metadata_df: sql.DataFrame,
    ascat_metadata_df: sql.DataFrame,
    segment_cnv_metadata_df: sql.DataFrame,
    segment_case_docs: Set[str],
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(
        segment_config.elasticsearch,
        spark_session,
        es_client,
        es_utils.MappingsLoader(),
        es_utils.SchemaLoader(),
    )
    df = case.CaseBuilder(
        segment_config.builders.case,
        spark_session,
        es_dataframe_util,
        es_utils.CaseFieldSelector(),
    ).build(
        maf_metadata_df=maf_metadata_df,
        ascat_metadata_df=ascat_metadata_df,
        segment_cnv_metadata_df=segment_cnv_metadata_df,
    )

    return df


@pytest.fixture(scope="package")
def segment_cnv_metadata_df(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    segment_file_docs: Set[str],
    ascat_metadata_df: sql.DataFrame,
) -> sql.DataFrame:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        segment_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    df = segment_cnv_metadata.SegmentCNVMetadataBuilder(
        segment_config.builders.segment_cnv_metadata,
        spark_session,
        es_dataframe_util,
    ).build(ascat_metadata_df=ascat_metadata_df)

    return df


@pytest.fixture(scope="package")
def segment_cnv_df(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    indexd: client.IndexClient,
    segment_cnv_metadata_df: sql.DataFrame,
) -> sql.DataFrame:
    df = segment_cnv.SegmentCNVBuilder(
        segment_config.builders.segment_cnv,
        spark_session,
        indexd_utils.DataFrameUtil(indexd, spark_session, mock.MagicMock()),
    ).build(segment_cnv_metadata_df=segment_cnv_metadata_df)

    return df
