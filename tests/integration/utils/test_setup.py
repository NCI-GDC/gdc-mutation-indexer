import collections
import gzip
import json
import logging
import os
import pathlib
import types
from collections.abc import Container, Iterable, Iterator, Mapping, Set
from importlib import resources
from typing import Any, ContextManager, Optional, Type, TypeVar, Union
from unittest import mock

import elasticsearch
from elasticsearch import helpers

from mutation_indexer import configuration, es_utils
from mutation_indexer.constants import app, build
from tests import integration

T = TypeVar("T")


def load_configuration(*overrides: Mapping[str, Any]) -> configuration.Configuration:
    with (
        resources.as_file(resources.files(integration) / "data/input") as input_dir,
        mock.patch.dict(
            os.environ,
            INPUT_DIR=str(input_dir),
            HOME="./",
            SPARK_HOME="./",
            TMPDIR="/tmp",
            MUTATION_INDEXER="./",
        ),
    ):
        files = (
            resources.files("mutation_indexer") / app.CONFIGURATION_FILE,
            resources.files(integration) / app.CONFIGURATION_FILE,
        )
        config_file = {"build": {"config_file": "dummy.toml"}}
        es_connection = {
            "elasticsearch": {"connection": {"nodes": os.environ["ES_NODES"]}}
        }

        return configuration.Configuration.load(
            *files, config_file, es_connection, *overrides
        )


def _remove_keys_from_dict(tree: T, remove_keys: Container[str]) -> T:
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


def remove_keys_from_dict(tree: dict, remove_keys: Optional[Container[str]]) -> dict:
    """
    Recursively remove keys from dictionary tree
    """
    if not remove_keys:
        return tree

    return _remove_keys_from_dict(tree, remove_keys)


class IndexManager(ContextManager["IndexManager"]):
    __slots__ = (
        "_es",
        "_logger",
        "_graph_indices",
        "_index_types",
        "_skip_creation",
        "_mappings_loader",
    )

    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
        logger: logging.Logger,
        index_types: Iterable[build.IndexType] = (
            build.IndexType.FILE,
            build.IndexType.CASE,
        ),
        skip_creation: bool = False,
    ) -> None:
        self._es = es
        self._logger = logger
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
            self._logger.info(f"Deleting existing index: {index_name}")
            self._es.indices.delete(index=index_name)
            self._es.indices.refresh()

        self._logger.info(f"Creating index: {index_name}")
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
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> Optional[bool]:
        for index_type in self._index_types:
            self._es.indices.delete(index=self._graph_indices[index_type], ignore=[404])

        return None


class DocumentLoader(ContextManager["DocumentLoader"]):
    __slots__ = ("_es", "_logger", "_graph_indices", "_id_fields", "_documents")

    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
        logger: logging.Logger,
    ) -> None:
        self._es = es
        self._logger = logger
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
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> Optional[bool]:
        for doc_type, ids in self._documents.items():
            if ids:
                index_name = self._graph_indices[doc_type]
                body = {"query": {"terms": {"file_id": list(ids)}}}
                self._es.delete_by_query(index=index_name, body=body, refresh=True)

        return None

    def _load_file(self, filename: str) -> Iterable[dict]:
        open_fn = gzip.open if filename.endswith(".gz") else open

        with open_fn(filename, "rt", encoding="utf-8") as f:
            if ".ndjson" in filename:
                docs = tuple(json.loads(l.strip()) for l in f)
            else:
                docs = json.load(f)

        return docs

    def _create_actions(
        self,
        inputs: Union[str, pathlib.Path, Iterable[dict]],
        index_name: str,
        index_type: build.IndexType,
    ) -> Iterator[dict]:
        doc_id = self._id_fields[index_type]
        docs = (
            self._load_file(str(inputs))
            if isinstance(inputs, (str, pathlib.Path))
            else inputs
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
        inputs: Union[str, pathlib.Path, Iterable[dict]],
    ) -> Set[str]:
        """Load documents from gzipped test data into test index.
        Default to the file named in ``conf.doc_files`` for the given ``doc_type``.
        Returns:
            A set containing the IDs of the documents that were inserted.
        """
        index_name = self._graph_indices[index_type]
        actions = tuple(self._create_actions(inputs, index_name, index_type))

        self._logger.info(f"Bulk loading {index_type} docs to the ES...")
        helpers.bulk(self._es, actions, ignore=409)

        self._es.indices.refresh(index=index_name)

        ids = frozenset(doc["_id"] for doc in actions)
        self._documents[index_type].update(ids)

        return ids
