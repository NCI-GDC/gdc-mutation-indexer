import elasticsearch
from pyspark import sql

from exports import configuration
from exports.constants import build
from tests.integration.utils import join_utils


def test_consequences_per_cnv(
    cnv_df: sql.DataFrame, cnv_centric_df: sql.DataFrame
) -> None:

    # Consequences per CNV built:
    df = join_utils.unpack_df_list(
        cnv_centric_df, "cnv_id", "consequence", "consequence_id"
    )
    cpc = join_utils.get_relationship_map(df, ("cnv_id", "consequence_id"))
    # Consequences per CNV expected:
    true_cpc = join_utils.get_relationship_map(cnv_df, ("cnv_id", "consequence_id"))

    assert cpc == true_cpc


def test_occurrences_per_cnv(
    cnv_df: sql.DataFrame, cnv_centric_df: sql.DataFrame
) -> None:
    # Occurrences per CNV built:
    df = join_utils.unpack_df_list(
        cnv_centric_df, "cnv_id", "occurrence", "occurrence_id"
    )
    opc = join_utils.get_relationship_map(df, ("cnv_id", "occurrence_id"))
    # Occurrences per CNV expected:
    true_opc = join_utils.get_relationship_map(cnv_df, ("cnv_id", "occurrence_id"))

    assert opc == true_opc


def test_cnv_centric_count(
    default_config: configuration.Configuration,
    es_client: elasticsearch.Elasticsearch,
    cnv_df: sql.DataFrame,
) -> None:
    expected_count = cnv_df.select("cnv_id").distinct().count()
    built_count = es_client.count(
        index=default_config.elasticsearch.write.indices[build.IndexType.CNV_CENTRIC]
    )

    assert built_count == expected_count
