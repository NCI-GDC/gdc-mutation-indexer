import pytest
import json
from pyspark.sql.functions import explode, lit, col

from exports.builders.utils import uuid5_col
from tests_config import TestConfig
from base_joins_test import BaseJoinsTest


conf = TestConfig()


@pytest.mark.usefixtures('gistic_df', 'case_df', 'cnv_centric_df')
class TestCNVCentricJoins(BaseJoinsTest):
    """
     cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                      |_____ case{}
                                |____ observation[]

    """

    def test_consequences_per_cnv(self, gistic_df, cnv_centric_df):

        # Consequences per CNV built:
        df = self.unpack_df_list(cnv_centric_df,
                                 'cnv_id', 'consequence', 'consequence_id')
        cpc = self.get_relationship_map(df, 'cnv_id', 'consequence_id')

        # Consequences per CNV expected:
        true_cpc = self.get_relationship_map(gistic_df,
                                             'cnv_id', 'consequence_id')
        assert cpc == true_cpc

    def test_occurrences_per_cnv(self, gistic_df, case_df, cnv_centric_df):
        # Occurrences per CNV built:
        df = self.unpack_df_list(cnv_centric_df,
                                 'cnv_id', 'occurrence', 'occurrence_id')
        opc = self.get_relationship_map(df, 'cnv_id', 'occurrence_id')

        # Occurrences per CNV expected:
        df = case_df.join(gistic_df, on='case_id', how='inner')
        true_ops = self.get_relationship_map(df, 'cnv_id', 'occurrence_id')
        
        assert opc == true_opc

