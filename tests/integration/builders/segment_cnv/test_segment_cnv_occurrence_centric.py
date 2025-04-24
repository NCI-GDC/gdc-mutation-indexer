"""Test creation of segment_cnv_occurrence_centric elasticsearch index."""

from collections.abc import Iterator

import elasticsearch
import pytest
from elasticsearch import helpers
from pyspark import sql

from mutation_indexer import es_utils
from mutation_indexer.builders import segment_cnv_occurrence_centric as scoc
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration


@pytest.fixture(scope="function")
def segment_cnv_occurrence_centric_builder(
    segment_config: configuration.Configuration,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> Iterator[scoc.SegmentCNVOccurrenceCentricBuilder]:
    mappings_loader = es_utils.MappingsLoader()
    es_dataframe_util = es_utils.DataFrameUtil(
        segment_config.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )

    yield scoc.SegmentCNVOccurrenceCentricBuilder(
        segment_config.builders.segment_cnv_occurrence_centric,
        spark_session,
        es_dataframe_util,
        mappings_loader,
    )


def test__segment_cnv_occurrence_centric_builder(
    segment_config: configuration.Configuration,
    segment_cnv_occurrence_centric_builder: scoc.SegmentCNVOccurrenceCentricBuilder,
    es_client: elasticsearch.Elasticsearch,
    segment_cnv_df: sql.DataFrame,
    case_df: sql.DataFrame,
) -> None:
    """Test creation of segment_cnv_occurrence_centric index.

    This is mainly a sanity check to make sure the entire data flow works as expected.
    """
    segment_cnv_occurrence_centric_index = segment_config.elasticsearch.write.indices[
        build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC
    ]
    segment_cnv_occurrence_centric_builder.build(
        segment_cnv_df=segment_cnv_df, case_df=case_df
    )
    es_client.indices.refresh()
    hits = tuple(helpers.scan(es_client, index=segment_cnv_occurrence_centric_index))
    distinct_file_ids = set()
    for hit in hits:
        for obs in hit["_source"]["case"]["observation"]:
            distinct_file_ids.add(obs["src_file_id"])

    assert len(hits) > 0
    assert len(distinct_file_ids) == 2
