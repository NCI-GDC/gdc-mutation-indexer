import pytest
from pyspark import sql

from exports import configuration, es_utils
from exports.configuration import elasticsearch
from exports.constants import build
from tests.integration.builders import base_joins_test


@pytest.mark.usefixtures("setup_graph_indices", "cnv_df", "cnv_centric_df")
class TestCNVCentricJoins(base_joins_test.BaseJoinsTest):
    """
    cnv{}
       |____ consequence[]
       |             |_____ gene{}
       |____ occurrence[]
                     |_____ case{}
                               |____ observation[]

    """

    def test_consequences_per_cnv(
        self, cnv_df: sql.DataFrame, cnv_centric_df: sql.DataFrame
    ) -> None:

        # Consequences per CNV built:
        df = self.unpack_df_list(
            cnv_centric_df, "cnv_id", "consequence", "consequence_id"
        )
        cpc = self.get_relationship_map(df, "cnv_id", "consequence_id")

        # Consequences per CNV expected:
        true_cpc = self.get_relationship_map(cnv_df, "cnv_id", "consequence_id")
        assert cpc == true_cpc

    def test_occurrences_per_cnv(
        self, cnv_df: sql.DataFrame, cnv_centric_df: sql.DataFrame
    ) -> None:
        # Occurrences per CNV built:
        df = self.unpack_df_list(
            cnv_centric_df, "cnv_id", "occurrence", "occurrence_id"
        )
        opc = self.get_relationship_map(df, "cnv_id", "occurrence_id")

        # Occurrences per CNV expected:
        true_opc = self.get_relationship_map(cnv_df, "cnv_id", "occurrence_id")
        assert opc == true_opc


def test_cnv_centric_count(
    default_config: configuration.Configuration,
    es_client: elasticsearch.Elasticsearch,
    cnv_df: sql.DataFrame,
) -> None:
    expected_count = cnv_df.select("cnv_id").distinct().count()
    built_count = es_utils.get_es_doc_count(
        es_client,
        default_config.elasticsearch.write.indices[build.IndexType.CNV_CENTRIC],
    )

    assert built_count == expected_count
