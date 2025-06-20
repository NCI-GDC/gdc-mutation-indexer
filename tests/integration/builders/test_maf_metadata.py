import contextlib
import dataclasses
from collections.abc import Iterable, Iterator, Sequence
from unittest import mock

import elasticsearch
import pytest
from elasticsearch import helpers

from mutation_indexer.builders import maf_metadata
from mutation_indexer.configuration import elasticsearch as es_config

FILE_SETTINGS = {
    "index": {
        "mapping": {
            "nested_fields": {"limit": "100"},
            "nested_objects": {"limit": "100000000"},
            "total_fields": {"limit": "1500"},
        },
        "refresh_interval": "1m",
        "number_of_shards": 12,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "edge_ngram": {
                    "min_gram": "1",
                    "side": "front",
                    "type": "edge_ngram",
                    "max_gram": "20",
                }
            },
            "normalizer": {
                "clinical_normalizer": {
                    "filter": ["lowercase"],
                    "type": "custom",
                    "char_filter": [],
                }
            },
            "analyzer": {
                "autocomplete_analyzed": {
                    "filter": ["lowercase", "edge_ngram"],
                    "tokenizer": "standard",
                },
                "autocomplete_prefix": {
                    "filter": ["lowercase", "edge_ngram"],
                    "tokenizer": "keyword",
                },
                "lowercase_keyword": {"filter": ["lowercase"], "tokenizer": "keyword"},
            },
        },
    },
}
FILE_MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        "analysis": {"properties": {"workflow_type": {"type": "keyword"}}},
        "cases": {
            "type": "nested",
            "properties": {
                "project": {"properties": {"project_id": {"type": "keyword"}}},
            },
        },
        "data_format": {"type": "keyword", "normalizer": "clinical_normalizer"},
        "data_type": {"type": "keyword"},
        "experimental_strategy": {"type": "keyword"},
        "file_id": {"type": "keyword", "normalizer": "clinical_normalizer"},
        "acl": {"type": "keyword", "normalizer": "clinical_normalizer"},
    },
}
TEST_INDEX = "test_maf_metadata_builder"
PRIORITIZED_STRATEGIES = ("WXS", "Targeted Sequencing")


@dataclasses.dataclass(frozen=True)
class Analysis:
    workflow_type: str = "Aliquot Ensemble Somatic Variant Merging and Masking"


@dataclasses.dataclass(frozen=True)
class Project:
    project_id: str = "GDC-TEST"


@dataclasses.dataclass(frozen=True)
class Case:
    project: Project = Project()


@dataclasses.dataclass(frozen=True)
class File:
    file_id: str
    acl: Sequence[str] = ("open",)
    data_format: str = "MAF"
    data_type: str = "Masked Somatic Mutation"
    experimental_strategy: str = "WXS"
    analysis: Analysis = Analysis()
    cases: tuple[Case, ...] = (Case(),)


@pytest.fixture(scope="class")
def maf_metadata_file_index(es_client: elasticsearch.Elasticsearch) -> Iterator[None]:
    try:
        es_client.indices.create(
            index=TEST_INDEX, settings=FILE_SETTINGS, mappings=FILE_MAPPINGS
        )
        yield None
    finally:
        es_client.indices.delete(index=TEST_INDEX, ignore_unavailable=True)


@pytest.mark.usefixtures("maf_metadata_file_index")
class TestMAFFileFilterFactory:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(self, es_client: elasticsearch.Elasticsearch) -> None:
        self.es_client = es_client

    def arrange_config(self) -> es_config.Read:
        return mock.MagicMock(spec=es_config.Read, file_index=TEST_INDEX)

    @contextlib.contextmanager
    def load_files(self, files: Iterable[File]) -> Iterator[None]:
        actions = (
            {
                "_id": file.file_id,
                "_index": TEST_INDEX,
                "_source": dataclasses.asdict(file),
            }
            for file in files
        )

        helpers.bulk(self.es_client, actions)
        self.es_client.indices.refresh(index=TEST_INDEX)

        yield None

        self.es_client.delete_by_query(
            index=TEST_INDEX, body={"query": {"match_all": {}}}, refresh=True
        )

    def test__get_filters__masked_somatic_mutations(self) -> None:
        files = (File(file_id="file-0"),)
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=("GDC-TEST",),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-0",)), f"{filters}"

    def test__get_filters__aggregated_somatic_mutations(self) -> None:
        files = (
            File(
                file_id="file-0",
                data_type="Aggregated Somatic Mutation",
                analysis=Analysis(
                    workflow_type="FoundationOne Variant Aggregation and Masking"
                ),
            ),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=("GDC-TEST",),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-0",)), f"{filters}"

    def test__get_filters__skip_non_mafs(self) -> None:
        files = (
            File(file_id="file-0", data_format="Star Count"),
            File(file_id="file-1"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert "file-0" not in result_ids, f"{filters}"

    def test__get_filters__skip_non_masked_or_aggregated_somatic_mutation(
        self,
    ) -> None:
        files = (
            File(file_id="file-0"),
            File(file_id="file-1", data_type="Other"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert "file-1" not in result_ids, f"{filters}"

    def test__get_filters__skip_masked_somatic_mutation_with_wrong_analysis(
        self,
    ) -> None:
        files = (
            File(file_id="file-0"),
            File(
                file_id="file-1",
                analysis=Analysis(
                    workflow_type="FoundationOne Variant Aggregation and Masking"
                ),
            ),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert "file-1" not in result_ids, f"{filters}"

    def test__get_filters__skip_aggregated_somatic_mutation_with_wrong_analysis(
        self,
    ) -> None:
        files = (
            File(file_id="file-0", data_type="Aggregated Somatic Mutation"),
            File(file_id="file-1"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert "file-0" not in result_ids, f"{filters}"

    def test__get_filters__all_projects(self) -> None:
        files = (
            File(file_id="file-0"),
            File(
                file_id="file-1",
                cases=(Case(project=Project(project_id="GDC-TEST-ALT")),),
            ),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-0", "file-1")), f"{filters}"

    def test__get_filters__single_project(self) -> None:
        files = (
            File(
                file_id="file-0",
            ),
            File(
                file_id="file-1",
                cases=(Case(project=Project(project_id="GDC-TEST-ALT")),),
            ),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=("GDC-TEST",),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-0",)), f"{filters}"

    def test__get_filters__select_wxs_over_targeted_sequencing(self) -> None:
        files = (
            File(file_id="file-0", experimental_strategy="Targeted Sequencing"),
            File(file_id="file-1"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-1",)), f"{filters}"

    def test__get_filters__select_targeted_sequencing_if_only_one(self) -> None:
        files = (
            File(file_id="file-0", experimental_strategy="Targeted Sequencing"),
            File(file_id="file-1", experimental_strategy="Targeted Sequencing"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset(("file-0", "file-1")), f"{filters}"

    def test__get_filters__skip_non_wxs_or_targeted_sequencing(self) -> None:
        files = (
            File(file_id="file-0"),
            File(file_id="file-1", experimental_strategy="Genotyping Array"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert "file-1" not in result_ids, f"{filters}"

    def test__get_filters__raise_runtime_error_if_no_matching_files(self) -> None:
        files = (File(file_id="file-0", data_type="Other"),)
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with (
            self.load_files(files),
            pytest.raises(
                RuntimeError,
                match=r"Invalid Data: No projects associated with any MAF files\.",
            ),
        ):
            _ = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )

    def test__get_filters__select_matching_acl_only(self) -> None:
        files = (
            File(
                file_id="file-0",
                acl=("secret",),
                experimental_strategy="Targeted Sequencing",
            ),
            File(file_id="file-1", experimental_strategy="Targeted Sequencing"),
        )
        config = self.arrange_config()
        builder = maf_metadata.MAFFileFilterFactory(config, self.es_client)

        with self.load_files(files):
            filters = builder.get_filters(
                acl=("open",),
                projects=(),
                prioritized_experimental_strategies=PRIORITIZED_STRATEGIES,
            )
            query = {"bool": {"must": filters}}

            result_ids = frozenset(
                hit["_source"]["file_id"]
                for hit in self.es_client.search(
                    index=TEST_INDEX, query=query, _source=["file_id"]
                )["hits"]["hits"]
            )

        assert result_ids == frozenset({"file-1"}), f"{filters}"
