from typing import List, Sequence

import elasticsearch
import pytest
from pyspark import sql

from mutation_indexer import builders, configuration, es_utils
from tests.integration.utils import test_setup


class TestCaseBuilder:
    """Test the CaseBuilder functionality for extracting the graph index"""

    def test_case_build(
        self,
        es_client: elasticsearch.Elasticsearch,
        case_df: sql.DataFrame,
        default_config: configuration.Configuration,
    ) -> None:
        read_config = default_config.elasticsearch.read
        expected_count = es_client.count(index=read_config.case_index)["count"]

        assert case_df.count() == expected_count

    def test_case_columns(self, case_df: sql.DataFrame) -> None:
        """Test that the right properties were loaded from case docs"""
        assert "case_id" in case_df.columns
        assert "files" not in case_df.columns
        # Make sure the sample_ids, slide_ids are not present
        assert "_ids" not in ",".join(case_df.columns)

    def test_number_of_cases(self, case_df: sql.DataFrame, all_cases: Sequence) -> None:
        """
        Check if case_df has correct number of lines
        It should include all cases in the GDC graph
        """
        assert case_df.count() == len(all_cases)

    @pytest.mark.parametrize(
        "projects, expected_count",
        [
            (["TCGA-KICH"], 6),
            (["TCGA-KIRP", "TCGA-SKCM"], 9),
            (["TCGA-TEST-NO-DATA"], 0),
        ],
    )
    def test_project_filter(
        self,
        projects: List[str],
        expected_count: int,
        spark_session: sql.SparkSession,
        maf_metadata_df: sql.DataFrame,
        maf_df: sql.DataFrame,
        cnv_df: sql.DataFrame,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        """Test filtering the projects included in the case DF.
        Confirm that the expected number of cases are extracted and that
        all cases are in one of the expected projects.
        """

        def load_config(data: dict) -> dict:
            data["build"]["projects"] = projects
            return data

        conf = test_setup.load_configuration(load_config)
        es_dataframe_util = es_utils.DataFrameUtil(
            conf.elasticsearch, spark_session, es_client, es_utils.MappingsLoader()
        )
        field_selector = es_utils.CaseFieldSelector()
        df = builders.CaseBuilder(
            conf.builders.viz.case, spark_session, es_dataframe_util, field_selector
        ).build(maf_metadata_df=maf_metadata_df, maf_df=maf_df, ascat_df=cnv_df)

        assert df.count() == expected_count
        for row in df.collect():
            assert row.project.project_id in projects
