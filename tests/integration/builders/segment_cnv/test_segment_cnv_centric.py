"""Test creation of segment_cnv_centric elasticsearch index."""

import logging
from collections.abc import Iterator

import elasticsearch
import pytest
from elasticsearch import helpers
from pyspark import sql

from mutation_indexer import configuration, es_utils
from mutation_indexer.builders import segment_cnv_centric
from mutation_indexer.constants import build

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def segment_cnv_centric_builder(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> Iterator[segment_cnv_centric.SegmentCNVCentricBuilder]:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        segment_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )

    yield segment_cnv_centric.SegmentCNVCentricBuilder(
        segment_config.builders.viz.segment_cnv_centric,
        spark_session,
        es_dataframe_util,
        mappings_loader,
    )

    # Delete the index
    segment_cnv_centric_index = segment_config.elasticsearch.write.indices[
        build.IndexType.SEGMENT_CNV_CENTRIC
    ]
    es_client.indices.delete(index=segment_cnv_centric_index, ignore_unavailable=True)


def test__segment_cnv_centric_builder(
    segment_config: configuration.Configuration,
    segment_cnv_centric_builder: segment_cnv_centric.SegmentCNVCentricBuilder,
    es_client: elasticsearch.Elasticsearch,
    segment_cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
) -> None:
    """Test creation of segment_cnv_centric index.

    This is mainly a sanity check to make sure the entire data flow works as expected.
    """
    segment_cnv_centric_index = segment_config.elasticsearch.write.indices[
        build.IndexType.SEGMENT_CNV_CENTRIC
    ]
    segment_cnv_centric_builder.build(segment_cnv_df=segment_cnv_df, case_df=case_df)
    es_client.indices.refresh()
    hits = tuple(helpers.scan(es_client, index=segment_cnv_centric_index))

    assert len(hits) > 0
