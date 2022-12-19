import contextlib
import logging
from typing import Any, Callable, ContextManager, Iterator

import elasticsearch
import pytest

from exports import configuration
from exports.builders import ascat
from exports.constants import build
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
            "data_type": "Gene Level Copy Number",
            "experimental_strategy": "Genotyping Array",
            "analysis": {"workflow_type": "ASCAT2"},
            "cases": [{"project": {"program": {"name": "TCGA"}}}],
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(())

        assert ids == ("ascat2-0",)

    def test__get_ids__ascat_ngs(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        doc = {
            "file_id": "ascat-ngs-0",
            "data_type": "Gene Level Copy Number",
            "experimental_strategy": "WGS",
            "analysis": {"workflow_type": "AscatNGS"},
            "cases": [{"project": {"program": {"name": "TCGA"}}}],
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(())

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
            "data_type": data_type,
            "experimental_strategy": experimental_strategy,
            "analysis": {"workflow_type": workflow_type},
            "cases": [{"project": {"program": {"name": "TCGA"}}}],
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(())

        assert ids == ()

    def test__get_ids__only_tcga_program(
        self,
        load_docs: LoadDocs,
        default_config: configuration.Configuration,
        es_client: elasticsearch.Elasticsearch,
    ) -> None:
        doc = {
            "file_id": "ascat-ngs-0",
            "data_type": "Gene Level Copy Number",
            "experimental_strategy": "WGS",
            "analysis": {"workflow_type": "AscatNGS"},
            "cases": [{"project": {"program": {"name": "GDC"}}}],
        }

        with load_docs(doc):
            resolver = ascat.DocumentResolver(
                default_config.elasticsearch.read, es_client
            )
            ids = resolver.get_ids(())

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
            ids = resolver.get_ids(("GDC-TEST", "TCGA-TEST1"))

        assert ids == ("ascat2-1",)
