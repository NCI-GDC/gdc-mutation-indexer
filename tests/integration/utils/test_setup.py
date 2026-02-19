import collections
import contextlib
import gzip
import json
import logging
import os
import pathlib
import types
from collections.abc import Container, Iterable, Iterator, Mapping, Set
from importlib import abc, resources
from typing import Any, TypeVar

import elasticsearch
from elasticsearch import helpers

from mutation_indexer import configuration, es_utils, gene_expression, viz
from mutation_indexer.constants import app, build

T = TypeVar("T")
TConfig = TypeVar("TConfig", bound=configuration.Configuration)

logger = logging.getLogger(__name__)


def _load_config[TConfig](
    configuration: type[TConfig],
    test_config: Iterable[abc.Traversable],
    overrides: Iterable[Mapping[str, Any]],
) -> TConfig:
    """Loads the default configuration along with any supplied overrides.

    Args:
        overrides: Values which should replace any of the default configurations.

    Returns:
        A configuration with the default values or the supplied overrides.
    """
    es_config = {
        "elasticsearch": {
            "connection": {
                "nodes": os.environ.get("ES_NODES", "localhost"),
            },
        }
    }

    return configuration.load(
        *configuration._default_files(), *test_config, es_config, *overrides
    )


def load_viz_config(*overrides: Mapping[str, Any]) -> viz.Configuration:
    test_configs = (
        resources.files("tests.integration") / app.CONFIGURATION_FILE,
        resources.files("tests.integration.viz") / app.CONFIGURATION_FILE,
    )

    return _load_config(viz.Configuration, test_configs, overrides)


def load_ge_config(*overrides: Mapping[str, Any]) -> gene_expression.Configuration:
    test_configs = (
        resources.files("tests.integration") / app.CONFIGURATION_FILE,
        resources.files("tests.integration.gene_expression") / app.CONFIGURATION_FILE,
    )

    return _load_config(gene_expression.Configuration, test_configs, overrides)


def _remove_keys_from_dict[T](tree: T, remove_keys: Container[str]) -> T:
    if isinstance(tree, dict):
        return {
            key: _remove_keys_from_dict(tree[key], remove_keys)
            for key in tree
            if key not in remove_keys
        }  # type: ignore
    elif isinstance(tree, list):
        return [_remove_keys_from_dict(element, remove_keys) for element in tree]  # type: ignore
    else:
        return tree


def remove_keys_from_dict(tree: dict, remove_keys: Container[str] | None) -> dict:
    """
    Recursively remove keys from dictionary tree
    """
    if not remove_keys:
        return tree

    return _remove_keys_from_dict(tree, remove_keys)


class IndexManager(contextlib.AbstractContextManager["IndexManager"]):
    __slots__ = ("_es", "_graph_indices", "_index_types", "_mappings_loader", "_skip_creation")

    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
        index_types: Iterable[build.IndexType] = (
            build.IndexType.FILE,
            build.IndexType.CASE,
        ),
        skip_creation: bool = False,
    ) -> None:
        self._es = es
        self._graph_indices = {
            build.IndexType.FILE: config.elasticsearch.read.file_index,
            build.IndexType.CASE: config.elasticsearch.read.case_index,
            **config.elasticsearch.write.indices,
        }
        self._index_types = index_types
        self._skip_creation = skip_creation
        self._mappings_loader = es_utils.MappingsLoader()

    def _create_index(self, index_type: build.IndexType) -> None:
        index_name = self._graph_indices[index_type]
        model_mapper = self._mappings_loader.load_mapper(index_type)

        if self._es.indices.exists(index=index_name):
            logger.info(f"Deleting existing index: {index_name}")
            self._es.indices.delete(index=index_name)
            self._es.indices.refresh()

        logger.info(f"Creating index: {index_name}")
        self._es.indices.create(
            index=index_name,
            settings=model_mapper.settings,
            mappings=model_mapper.mappings,
        )

    def __enter__(self) -> "IndexManager":
        """Creates all indices for the test suite"""
        if self._skip_creation:
            return self

        for index_type in self._index_types:
            self._create_index(index_type)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        for index_type in self._index_types:
            self._es.indices.delete(index=self._graph_indices[index_type], ignore=[404])

        return None


class DocumentLoader(contextlib.AbstractContextManager["DocumentLoader"]):
    __slots__ = ("_documents", "_es", "_graph_indices", "_id_fields")

    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
    ) -> None:
        self._es = es
        self._graph_indices = {
            build.IndexType.FILE: config.elasticsearch.read.file_index,
            build.IndexType.CASE: config.elasticsearch.read.case_index,
            **config.elasticsearch.write.indices,
        }
        self._id_fields = {
            build.IndexType.FILE: "file_id",
            build.IndexType.CASE: "case_id",
        }
        self._documents = collections.defaultdict(set)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        for doc_type, ids in self._documents.items():
            if ids:
                index_name = self._graph_indices[doc_type]
                body = {"query": {"ids": {"values": tuple(ids)}}}
                self._es.delete_by_query(index=index_name, body=body, refresh=True)

        return None

    def _load_file(self, filename: str) -> Iterable[dict]:
        open_fn = gzip.open if filename.endswith(".gz") else open

        with open_fn(filename, "rt", encoding="utf-8") as f:
            if ".ndjson" in filename:
                docs = tuple(json.loads(line.strip()) for line in f)
            else:
                docs = json.load(f)

        return docs

    def _create_actions(
        self,
        inputs: str | pathlib.Path | Iterable[dict],
        index_name: str,
        index_type: build.IndexType,
    ) -> Iterator[dict]:
        doc_id = self._id_fields[index_type]
        docs = (
            self._load_file(str(inputs)) if isinstance(inputs, (str, pathlib.Path)) else inputs
        )

        for doc in docs:
            doc = remove_keys_from_dict(doc, {"file_state"})
            action = {
                "_id": doc[doc_id],
                "_index": index_name,
                "_source": doc,
            }

            if index_type == "case":
                for _file in doc["files"]:
                    _file.pop("cases", None)

            yield action

    def load_docs(
        self,
        index_type: build.IndexType,
        inputs: str | pathlib.Path | Iterable[dict],
    ) -> Set[str]:
        """Load documents from gzipped test data into test index.
        Default to the file named in ``conf.doc_files`` for the given ``doc_type``.
        Returns:
            A set containing the IDs of the documents that were inserted.
        """
        index_name = self._graph_indices[index_type]
        actions = tuple(self._create_actions(inputs, index_name, index_type))

        logger.info(f"Bulk loading {index_type} docs to the ES...")
        helpers.bulk(self._es, actions, ignore=409)

        self._es.indices.refresh(index=index_name)

        ids = frozenset(doc["_id"] for doc in actions)
        self._documents[index_type].update(ids)

        return ids
