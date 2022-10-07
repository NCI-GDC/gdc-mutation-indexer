import collections
import functools
import gzip
import json
import logging
import types
from typing import (
    AbstractSet,
    Callable,
    Container,
    ContextManager,
    DefaultDict,
    Iterable,
    Iterator,
    Optional,
    Set,
    Type,
    TypeVar,
)

import elasticsearch
import importlib_resources as resources
import ndjson
import toml
from elasticsearch import helpers
from normalizer import mapper

from exports import configuration

T = TypeVar("T")


def load_configuraiton(
    *pre_load: Callable[[dict], dict]
) -> configuration.Configuration:
    data = toml.loads(resources.read_text("exports", "configuration.toml"))
    data = functools.reduce(lambda d, f: f(d), pre_load, data)

    return configuration.CONFIG_SCHEMA.load(data)


def _remove_keys_from_dict(tree: T, remove_keys: Container[str]) -> T:
    if isinstance(tree, dict):
        return {
            key: _remove_keys_from_dict(tree[key], remove_keys)
            for key in tree
            if key not in remove_keys
        }
    elif isinstance(tree, list):
        return [_remove_keys_from_dict(element, remove_keys) for element in tree]
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
    _doc_types = ("file", "case")

    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
        logger: logging.Logger,
    ) -> None:
        self._es = es
        self._logger = logger
        self._graph_indices = {
            "file": config.elasticsearch.read.file_index,
            "case": config.elasticsearch.read.case_index,
        }

    def _create_index(self, doc_type: str) -> None:
        index_name = self._graph_indices[doc_type]
        model_mapper = mapper.ModelMapper("gdc_from_graph", doc_type)

        if self._es.indices.exists(index_name):
            self._logger.info("Deleting existing index: {}".format(index_name))
            self._es.indices.delete(index_name)
            self._es.indices.refresh()

        self._logger.info("Creating index: {}".format(index_name))
        self._es.indices.create(index=index_name, body=model_mapper.index_settings)

    def __enter__(self) -> "IndexManager":
        """Creates all indices for the test suite"""
        for doc_type in self._doc_types:
            self._create_index(doc_type)

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> Optional[bool]:
        for doc_type in self._doc_types:
            self._es.indices.delete(index=self._graph_indices[doc_type], ignore=[404])

        return None


class DocumentLoader(ContextManager["DocumentLoader"]):
    def __init__(
        self,
        config: configuration.Configuration,
        es: elasticsearch.Elasticsearch,
        logger: logging.Logger,
    ) -> None:
        self._es = es
        self._logger = logger
        self._graph_indices = {
            "file": config.elasticsearch.read.file_index,
            "case": config.elasticsearch.read.case_index,
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
        loader = ndjson if ".ndjson" in filename else json
        open_fn = gzip.open if filename.endswith(".gz") else open

        with open_fn(filename, "rt", encoding="utf-8") as f:
            docs = loader.load(f)

        return docs

    def _create_actions(
        self, filename: str, index_name: str, doc_type: str
    ) -> Iterator[dict]:
        doc_id = f"{doc_type}_id"

        for doc in self._load_file(filename):
            doc = remove_keys_from_dict(doc, {"file_state"})
            action = {
                "_id": doc[doc_id],
                "_index": index_name,
                "_source": doc,
            }

            if doc_type == "case":
                for _file in doc["files"]:
                    _file.pop("cases", None)

            yield action

    def load_docs(self, doc_type: str, input_path: str) -> AbstractSet[str]:
        """Load documents from gzipped test data into test index.

        Default to the file named in ``conf.doc_files`` for the given ``doc_type``.

        Returns:
            A set containing the IDs of the documents that were inserted.
        """
        index_name = self._graph_indices[doc_type]
        actions = tuple(self._create_actions(input_path, index_name, doc_type))

        self._logger.info(f"Bulk loading {doc_type} docs to the ES...")
        helpers.bulk(self._es, actions, ignore=409)

        self._es.indices.refresh(index=index_name)

        ids = frozenset(doc["_id"] for doc in actions)
        self._documents[doc_type].update(ids)

        return ids
