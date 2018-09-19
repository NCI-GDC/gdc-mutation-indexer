import pytest
import os
# import re
# import json
# import yaml
# from collections import Counter
# from pyspark.sql.types import ArrayType, StringType
# 
from exports.builders import GisticBuilder
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'gistic_df')
class TestGisticBuilder:

    @pytest.fixture
    def builder(self, sqlContext):
        return GisticBuilder(conf, sqlContext)

    @pytest.fixture
    def expected_counts(self, builder):
        # project: line_count
        expected_counts = {}
        for url in conf.gistic_urls:
            project_name = os.path.basename(url).split('.')[0]

            df = builder.read(url)

            gene_count = df.count()
            aliquot_count = len(df.columns) - 3  # NOTE there are 3 non aliquot columns
            expected_counts.setdefault(project_name, 0)
            expected_counts[project_name] += gene_count * aliquot_count

        yield expected_counts

    @pytest.mark.gistic_combine
    def test_combine(self, builder, expected_counts):
        """
        Test that gistic files are combined correctly
        """

        combined_df = builder.combine()

        # Test total number of lines
        assert combined_df.count() == sum(expected_counts.values())

    @pytest.mark.gistic_build
    def test_build(self, gistic_df):
        """
        Test build output format
        """
        required_fields = [
            'cnv_change', 'gene_id', 'aliquot_id', 'case_id',
            'cnv_id', 'consequence_id', 'observation_id', 'occurrence_id',
        ]

        assert set(required_fields) - set(gistic_df.columns) == set()

    def test_cnv_change(self, gistic_df):
        """
        Test that cnv_change have only expected values
        """
        distinct = gistic_df.select('cnv_change').distinct()
        values = {r.cnv_change for r in distinct.collect()}
        expected_values = {'Shallow Loss', 'Deep Loss', 'Amplification', 'Gain'}
        assert values == expected_values

