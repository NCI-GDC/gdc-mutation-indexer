import collections
import functools
import json
import types
from typing import (
    AbstractSet,
    Container,
    Deque,
    Final,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import elasticsearch
import pyspark
from elasticsearch import helpers
from normalizer import mapper
from pyspark import sql

from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.constants import build


def iterate_es_results(
    es_client: elasticsearch.Elasticsearch,
    index_name: str,
    doc_type: Optional[str] = None,
    query: Optional[dict] = None,
) -> Iterable:
    """
    Returns iterator over elasticsearch query results
    """
    doc_iterator = helpers.scan(
        es_client,
        index=index_name,
        doc_type=doc_type,
        scroll="2m",
        size=100,
        query=query or {},
    )

    return doc_iterator


class MappingsLoader:
    """A class for loading the elasticsearch mapping for any given index."""

    __slots__ = ()

    def load_mappings(self, index_type: build.IndexType) -> dict:
        """
        Loads the mapping for the given index.

        Args:
            index_type: the index type associated with the elasticsearch mapping to
                load.

        Returns:
            A mappings dict based on the configured output of the given index type.
        """
        index_name, doc_type = index_type.get_mappings_details()
        model_mapper = mapper.ModelMapper(index_name, doc_type)

        return model_mapper.get_normalized_mappings()


def _is_included_field(
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    field: str,
) -> bool:
    """
    Determines if the given field should be included in the returned values based on the
    given excluded and included fields.

    Args:
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        field: the field in question.

    Returns:
        True if the field is a valid field and should be included in the resulting set
        of fields.
    """
    if field in excluded_fields:
        return False

    return included_fields is None or any(
        field.startswith(prefix) for prefix in included_fields
    )


def _convert_properties(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    path: str = "",
) -> Iterator[str]:
    """
    Converts all properties in the given mapping into flat fields which fall within the
    given included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path: the current path to the given set of properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    fields: Iterable[Tuple[str, Mapping]] = (
        (f"{path}{prop}", details) for prop, details in properties.items()
    )
    is_included_field = functools.partial(
        _is_included_field, excluded_fields, included_fields
    )
    fields = filter(lambda items: is_included_field(items[0]), fields)

    for field, details in fields:
        if "properties" in details:
            yield from _convert_properties(
                details["properties"],
                excluded_fields,
                included_fields,
                path=f"{field}.",
            )
        else:
            yield field


def _extract_fields(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    path_to_fields: Deque[str],
) -> Iterator[str]:
    """
    Extracts all fields which fall under the provided path and fall within the given
    included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path_to_fields: a series of properties which represent the path to the desired
            fields found within the given properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    if not properties:
        return

    if path_to_fields:
        next_prop = path_to_fields.popleft()
        properties = properties.get(next_prop, {}).get("properties", {})

        yield from _extract_fields(
            properties, excluded_fields, included_fields, path_to_fields
        )

    else:
        yield from _convert_properties(properties, excluded_fields, included_fields)


class CaseFieldSelector:
    """A class for selecting the case fields in a given elasticsearch index."""

    __slots__ = ("_mappings_loader",)

    CASE_PREFIXES: Final[Mapping[build.IndexType, str]] = types.MappingProxyType(
        {
            build.IndexType.CASE: "",
            build.IndexType.CASE_CENTRIC: "",
            build.IndexType.CNV_CENTRIC: "occurrence.case",
            build.IndexType.CNV_OCCURRENCE_CENTRIC: "case",
            build.IndexType.SSM_CENTRIC: "occurrence.case",
            build.IndexType.SSM_OCCURRENCE_CENTRIC: "case",
        }
    )

    def __init__(self, mappings_loader: Optional[MappingsLoader] = None) -> None:
        self._mappings_loader = mappings_loader or MappingsLoader()

    def _select_fields(
        self,
        index_type: build.IndexType,
        excluded_fields: Container[str],
        included_fields: Optional[Iterable[str]],
    ) -> AbstractSet[str]:
        if index_type not in self.CASE_PREFIXES:
            raise ValueError(f"Index: {index_type} is not supported.")

        prefix = self.CASE_PREFIXES[index_type]
        path_to_fields = (
            collections.deque(prefix.split(".")) if prefix else collections.deque()
        )
        mappings = self._mappings_loader.load_mappings(index_type)["mappings"]
        fields = _extract_fields(
            mappings["properties"], excluded_fields, included_fields, path_to_fields
        )

        return frozenset(fields)

    def select_for(
        self,
        *index_types: build.IndexType,
        excluded_fields: Container[str] = (),
        included_fields: Optional[Iterable[str]] = None,
    ) -> Iterable[str]:
        """
        Selects all common case fields found in the given indices.

        Args:
            *index_types: any indecies which should be included when selecting the case
                fields. Valid types: CASE_CENTRIC, CNV_CENTRIC, CNV_OCCURRENCE_CENTRIC,
                SSM_CENTRIC, and SSM_OCCURRENCE_CENTRIC
            excluded_fields: any fields which should be excluded in the selection. If
                a parent field is excluded then all of its children will be eg. if the
                exclusion is samples, then samples.sample_id is automatically excluded.
            included_fields: restricts the select to only included a subset of fields.
                this is useful when slecting fields nested under a particular parent.
                The default is to include all fields.
        """
        field_sets = (
            self._select_fields(index_type, excluded_fields, included_fields)
            for index_type in index_types
        )

        return functools.reduce(lambda set0, set1: set0 & set1, field_sets) | frozenset(
            ("case_id",)
        )


def _get_index(config: es_config.Elasticsearch, index_type: build.IndexType) -> str:
    if index_type == build.IndexType.FILE:
        return config.read.file_index

    if index_type == build.IndexType.CASE:
        return config.read.case_index

    if index_type in config.write.indices:
        return config.write.indices[index_type]

    raise ValueError(f"Index not configured: {index_type.name}")


class DataFrameUtil:
    __slots__ = ("_config", "_spark_session", "_es_client", "_mappings_loader")
    ES_FORMAT = "org.elasticsearch.spark.sql"

    def __init__(
        self,
        config: es_config.Elasticsearch,
        spark_session: sql.SparkSession,
        es_client: elasticsearch.Elasticsearch,
        mappings_loader: MappingsLoader,
    ) -> None:
        self._config = config
        self._spark_session = spark_session
        self._es_client = es_client
        self._mappings_loader = mappings_loader

    def _get_index(self, index_type: build.IndexType) -> str:
        return _get_index(self._config, index_type)

    def read(
        self,
        index_type: build.IndexType,
        include_fields: Union[Iterable[str], bool] = True,
        exclude_fields: Iterable[str] = (),
        include_as_arrays: Iterable[str] = (),
        query: Optional[dict] = None,
        read_metadata: bool = False,
    ) -> sql.DataFrame:
        """
        A utility for reading data from ES natively into spark.

        Args:
            index_type: The index from which the data will be loaded
            include_fields: The fields which will be included when reading
            exclude_fields: The fields which will not be included when reading
            include_as_arrays: The fields which need to be read as arrays and not
                simple types (e.g. field: ["this", "is", "example"])
                NOTE: This does NOT apply to arrays of objects
            query: The query to use in ES to limit the records returned

        Returns:
            A data frame containing the data from the elasticsearch index
        """
        index = self._get_index(index_type)
        reader = (
            self._spark_session.read.format(self.ES_FORMAT)
            .option("es.read.metadata", read_metadata)
            .option("es.nodes", self._config.connection.nodes)
            .option("es.net.http.auth.user", self._config.connection.user)
            .option("es.net.http.auth.pass", self._config.connection.password)
            .option("es.net.ssl", self._config.connection.use_ssl)
            .option("es.nodes.wan.only", True)
            .option("es.nodes.resolve.hostname", False)
            .option("es.resource.read", index)
            .option(
                "es.net.ssl.cert.allow.self.signed",
                not self._config.connection.verify_certs,
            )
            .option("es.nodes.resolve.hostname", False)
        )

        if query:
            reader = reader.option("es.query", json.dumps(query))

        if include_fields and isinstance(include_fields, Iterable):
            reader = reader.option("es.read.field.include", ",".join(include_fields))

        if exclude_fields and isinstance(exclude_fields, Iterable):
            reader = reader.option("es.read.field.exclude", ",".join(exclude_fields))

        if include_as_arrays and isinstance(include_as_arrays, Iterable):
            reader = reader.option(
                "es.read.field.as.array.include", ",".join(include_as_arrays)
            )

        return reader.load(index)

    def _create_index(self, index: str, index_type: build.IndexType) -> None:
        """
        Creates the index based on the mapping associated with the given index
        type.

        Args:
            index: the name of the index to be created
            index_type: the index type correlating to the mapping for the new index
        """
        if self._es_client.indices.exists(index=index):
            raise ValueError(
                f"Index: {index} already exists. Cannot overwrite existing index."
            )

        mappings = self._mappings_loader.load_mappings(index_type)

        self._es_client.indices.create(
            index=index, mappings=mappings["mappings"], settings=mappings["settings"]
        )

    def write(
        self, df: sql.DataFrame, index_type: build.IndexType, id_field: str
    ) -> None:
        """
        A utility for writing data from a data frame into elasticsearch.

        Args:
            df: the data frame which will be writen to elasticsearch for indexing
            index_type: the index type i.e. ssm_centric_index which the data will be
                written to
        """
        index = self._get_index(index_type)

        self._create_index(index, index_type)
        (
            df.write.format(self.ES_FORMAT)
            .option("es.nodes", self._config.connection.nodes)
            .option("es.net.http.auth.user", self._config.connection.user)
            .option("es.net.http.auth.pass", self._config.connection.password)
            .option("es.net.ssl", self._config.connection.use_ssl)
            .option(
                "es.net.ssl.cert.allow.self.signed",
                not self._config.connection.verify_certs,
            )
            .option("es.nodes.wan.only", "true")
            .option("es.nodes.resolve.hostname", "false")
            .option("es.resource.write", index)
            .option("es.http.timeout", "20m")
            .option("es.http.retries", "-1")
            .option("es.batch.write.retry.count", "-1")
            .option("es.batch.write.retry.wait", "10m")
            .option("es.batch.size.bytes", self._config.write.batch_size_bytes)
            .option("es.batch.size.entries", self._config.write.batch_size_entries)
            .option("es.batch.write.refresh", True)
            .option("es.mapping.id", id_field)
            .save(index)
        )


class RDDUtil:
    """
    A tool for loading spark RDD containing data loaded from an  elasticsearch index.

    CAUTION: In any case where the data has a regular schema and thus can be loaded
        using the DataframeUtil, default to loading the optimizable DataFrame object
        vs an RDD.
    """

    __slots__ = ("_config", "_spark_context")

    def __init__(
        self, config: es_config.Elasticsearch, spark_context: pyspark.SparkContext
    ) -> None:
        self._config = config
        self._spark_context = spark_context

    def _get_index(self, index_type: build.IndexType) -> str:
        return _get_index(self._config, index_type)

    def get_rdd(
        self,
        index_type: build.IndexType,
        include_fields: Union[Iterable[str], bool] = True,
        exclude_fields: Optional[Iterable[str]] = None,
        include_as_arrays: Iterable[str] = (),
        exclude_as_arrays: Iterable[str] = (),
        query: Optional[dict] = None,
        read_metadata: bool = False,
    ) -> pyspark.RDD:
        """
        A utility for loading data from ES natively into spark RDD objects.

        CAUTION: Use this only for loading data which cannot conform to a schema
            and thus cannot be loaded into a dataframe. All RDD objects should be
            standardized and converted into dataframes with `.toDF(SCHEMA)` ASAP in
            the process to maximize optimization of the spark program.

        Args:
            index: The index from which the data will be loaded
            include_fields: The fields which will be included when read
            include_as_arrays: The fields which need to be read as arrays and not
                simple types (e.g. field: ["this", "is", "example"])
                NOTE: This does NOT apply to arrays of objects
            query: The query to use in ES to limit the records returned

        Returns:
            An RDD dataset which contains the dynamic data returned by the query
            to the provided elasticsearch index.
        """
        config = {
            "es.read.metadata": str(read_metadata),
            "es.nodes": self._config.connection.nodes,
            "es.net.http.auth.user": self._config.connection.user,
            "es.net.http.auth.pass": self._config.connection.password,
            "es.net.ssl": str(self._config.connection.use_ssl),
            "es.net.ssl.cert.allow.self.signed": str(
                not self._config.connection.verify_certs
            ),
            "es.nodes.resolve.hostname": str(False),
            "es.resource": self._get_index(index_type),
        }

        if query:
            config["es.query"] = json.dumps(query)

        if include_fields and isinstance(include_fields, Iterable):
            config["es.read.field.include"] = ",".join(include_fields)

        if exclude_fields and isinstance(exclude_fields, Iterable):
            config["es.read.field.exclude"] = ",".join(exclude_fields)

        if include_as_arrays and isinstance(include_as_arrays, Iterable):
            config["es.read.field.as.array.include"] = ",".join(include_as_arrays)

        if exclude_as_arrays and isinstance(exclude_as_arrays, Iterable):
            config["es.read.field.as.array.exclude"] = ",".join(exclude_as_arrays)

        return self._spark_context.newAPIHadoopRDD(
            "org.elasticsearch.hadoop.mr.EsInputFormat",
            "org.apache.hadoop.io.NullWritable",
            "org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=config,
        )
