import collections
import logging
import types
from typing import (
    AbstractSet,
    Container,
    ContextManager,
    DefaultDict,
    Iterable,
    Optional,
    Set,
    Type,
)

import elasticsearch
from elasticsearch import helpers
from normalizer import mapper

from tests.integration import config
from tests.integration.utils import true_stats


def remove_keys(tree: dict, keys: Optional[Container[str]]) -> dict:
    if not keys:
        return tree

    if isinstance(tree, dict):
        return {key: remove_keys(tree[key], keys) for key in tree if key not in keys}
    elif isinstance(tree, list):
        return [remove_keys(element, keys) for element in tree]
    else:
        return tree


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

        if self._es.indices.exists(index_name):
            self._logger.info("Deleting existing index: {}".format(index_name))
            self._es.indices.delete(index_name)
            self._es.indices.refresh()

        self._logger.info("Creating index: {}".format(index_name))
        self._es.indices.create(index=index_name, body=model_mapper.index_settings)

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
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._es = es
        self._logger = logger
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

    def load_docs(
        self, doc_type: str, input_path: Optional[str] = None
    ) -> AbstractSet[str]:
        """Load documents from gzipped test data into test index.

        Default to the file named in ``conf.doc_files`` for the given ``doc_type``.

        Returns:
            A set containing the IDs of the documents that were inserted.
        """
        if not input_path:
            input_path = self._config.doc_files[doc_type]

        index_name = self._config.graph_indices[doc_type]

        docs = []
        for doc in true_stats.TestDataStats.load_es_graph_dump(input_path):
            to_append = {
                "_id": doc["{}_id".format(doc_type)],
                "_index": index_name,
                "_source": doc,
            }
            docs.append(to_append)

        # Remove .cases[] from underneath case.files[]
        if doc_type == "case":
            for doc in docs:
                for _file in doc["_source"]["files"]:
                    _file.pop("cases", None)

        # TODO: temp fix
        docs = remove_keys(docs, {"file_state"})

        self._logger.info(
            "Bulk loading {} docs to the ES... {}".format(doc_type, len(docs))
        )
        helpers.bulk(self._es, docs, ignore=409)

        self._logger.info("loaded {} {} docs".format(len(docs), doc_type))

        self._es.indices.refresh(index=index_name)

        ids = frozenset(doc["_id"] for doc in docs)
        self._documents[doc_type].update(ids)

        return ids
