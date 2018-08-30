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
            expected_counts.setdefault(project_name, 0)

            df = builder.read(url)
            expected_counts[project_name] += df.count()

        yield expected_counts

    @pytest.mark.gistic_combine
    def test_combine(self, builder, expected_counts):
        """
        Test that gistic files are combined correctly
        """

        combined_df = builder.combine(conf.maf_urls)

        # Test total number of lines
        assert combined_df.count() == sum(expected_counts.values())

    @pytest.mark.gistic_build
    def test_build(self, gistic_df):
        # TODO: define
        1/0

