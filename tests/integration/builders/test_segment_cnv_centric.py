"""Test creation of segment_cnv_centric elasticsearch index."""

import logging
import pathlib
from collections.abc import Iterable, Iterator, Set
from typing import Any, Optional
from unittest import mock

import elasticsearch
import more_itertools
import pytest
from elasticsearch import helpers
from indexclient import client
from pyspark import sql

from mutation_indexer import configuration, es_utils, indexd_utils
from mutation_indexer.builders import (
    ascat_metadata,
    case,
    segment_cnv,
    segment_cnv_centric,
    segment_cnv_metadata,
)
from mutation_indexer.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def indexd(input_dir: pathlib.Path) -> client.IndexClient:
    path = input_dir / "segment_cnv"
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
def segment_file_docs(
    default_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_graph_indices: Any,
) -> Iterator[Set[str]]:
    logger.info("Loading segment_cnv files to file index.")
    segment_file = input_dir / "segment_cnv-files.ndjson"

    with test_setup.IndexManager(
        default_config,
        es_client,
        logger,
        index_types=(build.IndexType.SEGMENT_CNV_CENTRIC,),
        skip_creation=True,
    ):
        with test_setup.DocumentLoader(default_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.FILE, segment_file)


@pytest.fixture(scope="module")
def ascat_file_docs(
    default_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_graph_indices: Any,
) -> Iterator[Set[str]]:
    logger.info("Loading ascat_metadata files to file index.")
    ascat_file = input_dir / "ascat_metadata-files.ndjson"

    with test_setup.IndexManager(
        default_config,
        es_client,
        logger,
        index_types=(build.IndexType.SEGMENT_CNV_CENTRIC,),
        skip_creation=True,
    ):
        with test_setup.DocumentLoader(default_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.FILE, ascat_file)


@pytest.fixture(scope="function")
def ascat_metadata_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    ascat_file_docs: Set[str],
) -> sql.DataFrame:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    es_rdd_util = es_utils.RDDUtil(
        default_config.elasticsearch, spark_session.sparkContext
    )
    df = ascat_metadata.ASCATMetadataBuilder(
        default_config.builders.viz.ascat_metadata,
        spark_session,
        es_dataframe_util,
        es_rdd_util,
    ).build()

    df.show()
    return df


@pytest.fixture(scope="module")
def segment_case_docs(
    default_config: configuration.Configuration,
    input_dir: pathlib.Path,
    es_client: elasticsearch.Elasticsearch,
    setup_graph_indices: Any,
) -> Iterator[Set[str]]:
    logger.info("Loading segment_cnv cases to case index.")
    segment_case = input_dir / "segment_cnv-cases.ndjson"

    with test_setup.IndexManager(
        default_config,
        es_client,
        logger,
        index_types=(build.IndexType.SEGMENT_CNV_CENTRIC,),
        skip_creation=True,
    ):
        with test_setup.DocumentLoader(default_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.CASE, segment_case)


@pytest.fixture(scope="function")
def case_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    maf_metadata_df: sql.DataFrame,
    ascat_metadata_df: sql.DataFrame,
    segment_case_docs: Set[str],
) -> sql.DataFrame:
    es_dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        es_client,
        es_utils.MappingsLoader(),
        es_utils.SchemaLoader(),
    )
    df = case.CaseBuilder(
        default_config.builders.viz.case,
        spark_session,
        es_dataframe_util,
        es_utils.CaseFieldSelector(),
    ).build(maf_metadata_df=maf_metadata_df, ascat_metadata_df=ascat_metadata_df)

    df.show()
    return df


@pytest.fixture(scope="function")
def segment_cnv_metadata_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
    segment_file_docs: Set[str],
    ascat_metadata_df: sql.DataFrame,
) -> sql.DataFrame:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    df = segment_cnv_metadata.SegmentCNVMetadataBuilder(
        default_config.builders.viz.segment_cnv_metadata,
        spark_session,
        es_dataframe_util,
    ).build(ascat_metadata_df=ascat_metadata_df)

    df.show()
    return df


@pytest.fixture(scope="function")
def segment_cnv_df(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    indexd: client.IndexClient,
    segment_cnv_metadata_df: sql.DataFrame,
) -> sql.DataFrame:
    df = segment_cnv.SegmentCNVBuilder(
        default_config.builders.viz.segment_cnv,
        spark_session,
        indexd_utils.DataFrameUtil(indexd, spark_session, mock.MagicMock()),
    ).build(segment_cnv_metadata_df=segment_cnv_metadata_df)

    df.show()
    return df


@pytest.fixture(scope="function")
def segment_cnv_centric_builder(
    default_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> Iterator[segment_cnv_centric.IndexBuilder]:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )

    yield segment_cnv_centric.IndexBuilder(
        default_config.builders.viz.segment_cnv_centric,
        spark_session,
        es_dataframe_util,
        mappings_loader,
    )

    # Delete the index
    segment_cnv_centric_index = default_config.elasticsearch.write.indices[
        build.IndexType.SEGMENT_CNV_CENTRIC
    ]
    es_client.indices.delete(index=segment_cnv_centric_index, ignore_unavailable=True)


def test__segment_cnv_centric_builder(
    default_config: configuration.Configuration,
    segment_cnv_centric_builder: segment_cnv_centric.IndexBuilder,
    es_client: elasticsearch.Elasticsearch,
    segment_cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
) -> None:
    """Test creation of segment_cnv_centric index."""
    segment_cnv_centric_index = default_config.elasticsearch.write.indices[
        build.IndexType.SEGMENT_CNV_CENTRIC
    ]
    segment_cnv_centric_builder.build(segment_cnv_df=segment_cnv_df, case_df=case_df)
    es_client.indices.refresh()

    hits = helpers.scan(es_client, index=segment_cnv_centric_index)
    segments = tuple(h["_source"] for h in hits)

    assert len(segments) == 2
