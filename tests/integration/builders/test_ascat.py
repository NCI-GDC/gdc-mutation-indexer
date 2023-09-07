import contextlib
import logging
from collections.abc import Callable, Iterator
from typing import Any, ContextManager

import elasticsearch
import pytest

from mutation_indexer import configuration
from mutation_indexer.builders import ascat
from mutation_indexer.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)

LoadDocs = Callable[..., ContextManager]


@pytest.fixture(scope="module")
def load_docs(
    default_config: configuration.Configuration, es_client: elasticsearch.Elasticsearch
) -> LoadDocs:
    @contextlib.contextmanager
    def _load_docs(*docs: dict) -> Iterator[Any]:
        with test_setup.DocumentLoader(default_config, es_client, logger) as loader:
            yield loader.load_docs(build.IndexType.FILE, docs)

    return _load_docs


@pytest.mark.usefixtures("setup_graph_indices")
class TestDocumentResolver:
    def test__get_ids__ascat2(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        doc = {
            "file_id": "ascat2-0",
            "acl": ("open",),
            "data_type": "Gene Level Copy Number",
            "experimental_strategy": "Genotyping Array",
            "analysis": {"workflow_type": "ASCAT2"},
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(("open",), ())

        assert ids == ("ascat2-0",)

    def test__get_ids__ascat_ngs(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        doc = {
            "file_id": "ascat-ngs-0",
            "acl": ("open",),
            "data_type": "Gene Level Copy Number",
            "experimental_strategy": "WGS",
            "analysis": {"workflow_type": "AscatNGS"},
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(("open",), ())

        assert ids == ("ascat-ngs-0",)

    @pytest.mark.parametrize(
        ("data_type", "experimental_strategy", "workflow_type"),
        (
            ("Other", "WGS", "AscatNGS"),
            ("Gene Level Copy Number", "WGS", "ASCAT2"),
            ("Gene Level Copy Number", "Genotyping Array", "AscatNGS"),
        ),
    )
    def test__get_ids__filter_wrong_ascats(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
        data_type: str,
        experimental_strategy: str,
        workflow_type: str,
    ) -> None:
        doc = {
            "file_id": "ascat-0",
            "acl": ("open",),
            "data_type": data_type,
            "experimental_strategy": experimental_strategy,
            "analysis": {"workflow_type": workflow_type},
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(("open",), ())

        assert ids == ()

    def test__get_ids__specified_projects(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        docs = (
            {
                "file_id": "ascat2-0",
                "acl": ("open",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "Genotyping Array",
                "analysis": {"workflow_type": "ASCAT2"},
                "cases": [
                    {
                        "project": {
                            "project_id": "TCGA-TEST0",
                            "program": {"name": "TCGA"},
                        }
                    }
                ],
            },
            {
                "file_id": "ascat2-1",
                "acl": ("open",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "Genotyping Array",
                "analysis": {"workflow_type": "ASCAT2"},
                "cases": [
                    {
                        "project": {
                            "project_id": "TCGA-TEST1",
                            "program": {"name": "TCGA"},
                        }
                    }
                ],
            },
            {
                "file_id": "ascat-ngs-0",
                "acl": ("open",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "WGS",
                "analysis": {"workflow_type": "AscatNGS"},
                "cases": [
                    {"project": {"project_id": "GDC-TEST", "program": {"name": "GDC"}}}
                ],
            },
        )

        with load_docs(*docs):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(("open",), ("GDC-TEST", "TCGA-TEST1"))

        assert ids == ("ascat2-1", "ascat-ngs-0")

    def test__get_ids__specified_acl(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        docs = (
            {
                "file_id": "ascat2-0",
                "acl": ("secret",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "Genotyping Array",
                "analysis": {"workflow_type": "ASCAT2"},
                "cases": [
                    {
                        "project": {
                            "project_id": "TCGA-TEST0",
                            "program": {"name": "TCGA"},
                        }
                    }
                ],
            },
            {
                "file_id": "ascat2-1",
                "acl": ("super-secret",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "Genotyping Array",
                "analysis": {"workflow_type": "ASCAT2"},
                "cases": [
                    {
                        "project": {
                            "project_id": "TCGA-TEST1",
                            "program": {"name": "TCGA"},
                        }
                    }
                ],
            },
            {
                "file_id": "ascat-ngs-0",
                "acl": ("open",),
                "data_type": "Gene Level Copy Number",
                "experimental_strategy": "WGS",
                "analysis": {"workflow_type": "AscatNGS"},
                "cases": [
                    {"project": {"project_id": "GDC-TEST", "program": {"name": "GDC"}}}
                ],
            },
        )

        with load_docs(*docs):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(("open", "secret"), ())

        assert ids == ("ascat2-0", "ascat-ngs-0")
