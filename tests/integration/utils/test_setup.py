import collections
import logging
import types
from typing import (
    Container,
    ContextManager,
    DefaultDict,
    Iterable,
    Iterator,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    overload,
)

import elasticsearch
from elasticsearch import helpers
from normalizer import mapper

from tests.integration import config
from tests.integration.utils import true_stats

T = TypeVar("T")


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
    def __init__(
        self,
        config: config.TestConfig,
        es: elasticsearch.Elasticsearch,
        logger: logging.Logger,
        doc_types: Iterable[str],
    ) -> None:
        self._config = config
        self._es = es
        self._logger = logger
        self._doc_types = doc_types

    def _create_index(self, doc_type: str) -> None:
        index_name = self._config.graph_indices[doc_type]
        model_mapper = mapper.ModelMapper("gdc_from_graph", doc_type)
        normalized_mappings = model_mapper.get_normalized_mappings()

        if self._es.indices.exists(index=index_name):
            self._logger.info(f"Deleting existing index: {index_name}")
            self._es.indices.delete(index_name)
            self._es.indices.refresh()

        self._logger.info(f"Creating index: {index_name}")
        self._es.indices.create(
            index=index_name,
            mappings=normalized_mappings["mappings"],
            settings=normalized_mappings["settings"],
        )

    def create_indices(self) -> bool:
        """Creates all indices for the test suite"""
        for doc_type in self._doc_types:
            self._create_index(doc_type)

        return True

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[types.TracebackType],
    ) -> Optional[bool]:
        for doc_type in self._doc_types:
            self._es.indices.delete(
                index=self._config.graph_indices[doc_type], ignore=[404]
            )

        return None


class DocumentLoader(ContextManager["DocumentLoader"]):
    def __init__(
        self,
        config: config.TestConfig,
        es: elasticsearch.Elasticsearch,
    ) -> None:
        self._config = config
        self._es = es
        self._documents = collections.defaultdict(
            set
        )  # type: DefaultDict[str, Set[str]]

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[types.TracebackType],
    ) -> Optional[bool]:
        for doc_type, ids in self._documents.items():
            if ids:
                index_name = self._config.graph_indices[doc_type]
                body = {"query": {"terms": {"file_id": list(ids)}}}
                self._es.delete_by_query(index=index_name, body=body, refresh=True)

        return None

    def _load_doc(self, doc_type: str, doc: dict) -> str:
        index = self._config.graph_indices[doc_type]
        doc_id = doc[f"{doc_type}_id"]

        self._es.create(index=index, id=doc_id, document=doc, refresh=True)

        self._documents[doc_type].add(doc_id)

        return doc_id

    def _create_actions(
        self, index: str, doc_id: str, docs: Iterable[dict]
    ) -> Iterator[dict]:
        for doc in docs:
            yield {
                "_id": doc[doc_id],
                "_index": index,
                "_source": doc,
            }

    def _load_docs(self, doc_type: str, docs: Iterable[dict]) -> Set[str]:
        index = self._config.graph_indices[doc_type]
        actions = self._create_actions(index, f"{doc_type}_id", docs)

        helpers.bulk(self._es, actions=actions, ignore=409)

        self._es.indices.refresh(index=index)

        ids = frozenset(doc["_id"] for doc in docs)
        self._documents[doc_type].update(ids)

        return ids

    @overload
    def load_docs(self, doc_type: str, data: dict) -> str:
        pass

    @overload
    def load_docs(self, doc_type: str, data: Iterable[dict]) -> Set[str]:
        pass

    @overload
    def load_docs(self, doc_type: str, data: str) -> Set[str]:
        pass

    def load_docs(
        self, doc_type: str, data: Union[str, dict, Iterable[dict]]
    ) -> Union[str, Set[str]]:
        """Load documents from a file or directly form the data provided into the given
        index.

        Args:
            doc_type: The index type into which the data will be interted.
            data: The data or the path to a file containing the data which will be
                inserted into the index assocated with the given index type.

        Returns:
            The ID(s) of the document(s) inserted.
        """
        if isinstance(data, dict):
            return self._load_doc(doc_type, data)

        if not isinstance(data, str):
            return self._load_docs(doc_type, data)

        docs = true_stats.TestDataStats.load_es_graph_dump(data)

        # Remove .cases[] from underneath case.files[]
        if doc_type == "case":
            for doc in docs:
                for _file in doc["files"]:
                    _file.pop("cases", None)

        # TODO: temp fix
        docs = (remove_keys_from_dict(doc, {"file_state"}) for doc in docs)

        return self._load_docs(doc_type, docs)
