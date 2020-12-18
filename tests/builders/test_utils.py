import os

import pytest

import tests_config
from exports.builders import utils


def _case_ids_from_df(case_id_df):
    return set(row.case_id for row in case_id_df.select('case_id').collect())


class TestUtils(object):
    """Tests for the mutation indexer's ``exports.builders.utils`` module.

    Not to be confused with utils for tests.
    """

    def test_get_case_ids_from_source_es(self, sqlContext):
        """Verify the expected case IDs are read from the test MAF headers and graph."""
        conf = tests_config.TestConfig()

        case_id_df = utils.get_case_ids_from_source_es(conf, sqlContext)

        # Rather than try to reconstruct this programmatically and create even
        # more code to test, here's what the test data should yield.
        expected_case_ids = {
            "13afbde8-e5b5-4f3c-8a9d-daef71560005",
            "1db41963-a520-47f0-828c-ed5c626507b1",
            "2f5d8110-35c7-419f-8b35-bc3040f940f3",
            "452135f2-6de6-4593-a091-ddf6344ee431",
            "68642658-7996-4423-bb25-d3beb9a414f1",
            "872092b3-d31e-44d7-bd03-e29f52f8ab5a",
            "a20aeafc-9a68-4af0-87ea-532ee835ebb2",
            "a29a20e3-5c2c-4f37-b93e-ae9ebc46ec53",
            "b08dfba8-6afb-4217-9259-72be6f1f3363",
            "bbbce1ba-c739-43ba-b9cf-a4f746491ae3",
            "c689ae1d-4a6b-45db-b4d1-6b34c5c61522",
            "d241a660-1c84-44fa-a6b3-ec9284333bd2",
            "d2748e35-4719-43c1-a533-b6b0cd9688c3",
            "e8c2a8c6-5c2b-460b-b536-60bc537e6be3",
            "ee8c1919-17a9-4df1-8aa5-79546621b23c",
            "f18cfe4a-fffd-4e09-9eef-343ba9ffd0d1",
        }

        assert _case_ids_from_df(case_id_df) == expected_case_ids

    def test_get_case_ids_from_source_es__project_filter(self, sqlContext):
        """Verify the case IDs are filtered based on the config."""
        conf = tests_config.TestConfig()
        conf.projects = ["TCGA-KICH"]

        case_id_df = utils.get_case_ids_from_source_es(conf, sqlContext)

        expected_case_ids = {
            "452135f2-6de6-4593-a091-ddf6344ee431",
            "872092b3-d31e-44d7-bd03-e29f52f8ab5a",
            "b08dfba8-6afb-4217-9259-72be6f1f3363",
            "c689ae1d-4a6b-45db-b4d1-6b34c5c61522",
            "e8c2a8c6-5c2b-460b-b536-60bc537e6be3",
            "ee8c1919-17a9-4df1-8aa5-79546621b23c",
        }

        assert _case_ids_from_df(case_id_df) == expected_case_ids

    @pytest.mark.usefixtures("index_cases_with_duplicate_aliquots")
    def test_get_case_ids_from_source_es__project_id_pragma(self, sqlContext):
        """Verify the right case IDs are chosen for a MAF with a project ID pragma.

        Provide a MAF with an aliquot submitter ID that is referenced by cases in
        multiple projects. Confirm mutation indexer associates the aliquot with the
        correct case based on the ``#project_id`` pragma.
        """
        conf = tests_config.TestConfig()
        bad_maf_path = os.path.join(
            conf.input_dir, 'maf', 'edge_cases', 'ambiguous_submitter_id.maf'
        )
        conf.maf_urls.append(bad_maf_path)

        case_id_df = utils.get_case_ids_from_source_es(conf, sqlContext)

        expected_case_ids = {
            "13afbde8-e5b5-4f3c-8a9d-daef71560005",
            "1db41963-a520-47f0-828c-ed5c626507b1",
            "2f5d8110-35c7-419f-8b35-bc3040f940f3",
            "452135f2-6de6-4593-a091-ddf6344ee431",
            "68642658-7996-4423-bb25-d3beb9a414f1",
            "872092b3-d31e-44d7-bd03-e29f52f8ab5a",
            "a20aeafc-9a68-4af0-87ea-532ee835ebb2",
            "a29a20e3-5c2c-4f37-b93e-ae9ebc46ec53",
            "b08dfba8-6afb-4217-9259-72be6f1f3363",
            "bbbbbbbb-aaaa-4ddd-dddd-00000000001b",  # case from BAD-GRAPH-B
            "bbbce1ba-c739-43ba-b9cf-a4f746491ae3",
            "c689ae1d-4a6b-45db-b4d1-6b34c5c61522",
            "d241a660-1c84-44fa-a6b3-ec9284333bd2",
            "d2748e35-4719-43c1-a533-b6b0cd9688c3",
            "e8c2a8c6-5c2b-460b-b536-60bc537e6be3",
            "ee8c1919-17a9-4df1-8aa5-79546621b23c",
            "f18cfe4a-fffd-4e09-9eef-343ba9ffd0d1",
        }

        assert _case_ids_from_df(case_id_df) == expected_case_ids

    @pytest.mark.usefixtures("index_cases_with_duplicate_aliquots")
    def test_get_case_ids_from_source_es__project_filter_and_pragma(self, sqlContext):
        """Test interaction between the project ID pragma and project filter.

        Try various project filters that may or may not include the project referenced
        in a ``#project_id`` pragma, and confirm the expected cases are returned.
        """
        conf = tests_config.TestConfig()
        bad_maf_path = os.path.join(
            conf.input_dir, 'maf', 'edge_cases', 'ambiguous_submitter_id.maf'
        )
        conf.maf_urls.append(bad_maf_path)

        expected_kich_ids = {
            "452135f2-6de6-4593-a091-ddf6344ee431",
            "872092b3-d31e-44d7-bd03-e29f52f8ab5a",
            "b08dfba8-6afb-4217-9259-72be6f1f3363",
            "c689ae1d-4a6b-45db-b4d1-6b34c5c61522",
            "e8c2a8c6-5c2b-460b-b536-60bc537e6be3",
            "ee8c1919-17a9-4df1-8aa5-79546621b23c",
        }

        expected_bad_graph_ids = {"bbbbbbbb-aaaa-4ddd-dddd-00000000001b"}

        conf.projects = ["TCGA-KICH"]
        kich_df = utils.get_case_ids_from_source_es(conf, sqlContext)
        assert _case_ids_from_df(kich_df) == expected_kich_ids

        conf.projects = ["BAD-GRAPH-B"]
        bad_graph_b_df = utils.get_case_ids_from_source_es(conf, sqlContext)
        assert _case_ids_from_df(bad_graph_b_df) == expected_bad_graph_ids

        conf.projects = ["BAD-GRAPH-B", "TCGA-KICH"]
        both_df = utils.get_case_ids_from_source_es(conf, sqlContext)
        assert _case_ids_from_df(both_df) == expected_bad_graph_ids | expected_kich_ids

        conf.projects = ["BAD-GRAPH-A", "BAD-GRAPH-C"]
        bad_graph_other_df = utils.get_case_ids_from_source_es(conf, sqlContext)
        assert _case_ids_from_df(bad_graph_other_df) == set()

    @pytest.mark.usefixtures("files_with_linked_cases")
    def test__get_case_file_metadata__tcg_kich(self, sqlContext):
        config = tests_config.TestConfig()
        config.projects = ["TCGA-KICH"]

        df = utils.get_case_file_metadata(sqlContext, config)

        files = {r.case_id: r.file_id for r in df.collect()}

        assert files.get("452135f2-6de6-4593-a091-ddf6344ee431") == "acc6c688-a233-46bf-b2d9-7bfec28241ed"
        assert files.get("872092b3-d31e-44d7-bd03-e29f52f8ab5a") == "cf3708a2-28e1-49cc-9e67-4416b3bf5b1a"
