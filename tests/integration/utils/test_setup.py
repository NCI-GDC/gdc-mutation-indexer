import collections
import functools
import gzip
import json
import logging
import pathlib
import types
from typing import (
    AbstractSet,
    Callable,
    Container,
    ContextManager,
    Iterable,
    Iterator,
    Optional,
    Type,
    TypeVar,
    Union,
)

import elasticsearch
import importlib_resources as resources
import toml
from elasticsearch import helpers
from normalizer import mapper

from exports import configuration
from exports.constants import build

T = TypeVar("T")


def load_configuraiton(
    *pre_load: Callable[[dict], dict]
) -> configuration.Configuration:
    data = toml.loads(resources.read_text("exports", "configuration.toml"))
    data = functools.reduce(lambda d, f: f(d), pre_load, data)

    return configuration.CONFIG_SCHEMA.load(data)  # type: ignore


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
    __slots__ = ("_es", "_logger", "_graph_indices", "_index_types")

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

    def _create_index(self, index_type: build.IndexType) -> None:
        index_name = self._graph_indices[index_type]
        model_mapper = mapper.ModelMapper(*index_type.get_mappings_details())
        mappings = model_mapper.get_normalized_mappings()

        if self._es.indices.exists(index=index_name):
            self._logger.info(f"Deleting existing index: {index_name}")
            self._es.indices.delete(index=index_name)
            self._es.indices.refresh()

        self._logger.info(f"Creating index: {index_name}")
        self._es.indices.create(
            index=index_name,
            settings=mappings["settings"],
            mappings=mappings["mappings"],
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
    ) -> AbstractSet[str]:
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
