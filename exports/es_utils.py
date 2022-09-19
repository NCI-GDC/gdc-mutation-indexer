import functools
import itertools
import json
import re
from typing import Any, Callable, Container, Dict, Iterable, Optional, Set, Union

import elasticsearch
import pyspark
from elasticsearch import helpers
from normalizer import mapper
from pyspark import sql

from exports.constants import build


def iterate_es_results(es_client, index_name, doc_type=None, query=None):
    """
    Returns iterator over elasticsearch query results
    """
    if query is None:
        query = {}

    doc_iterator = helpers.scan(
        es_client,
        index=index_name,
        doc_type=doc_type,
        scroll="2m",
        size=100,
        query=query,
    )
    return doc_iterator


def get_values_from_path(es_doc, path):
    """
    Retrieves the value(s) from the document's dot-delimited path

    NOTE: Since there could be array fields in :path, there can be multiple values
    at the :path in :es_doc
    """

    if isinstance(path, str):
        path = path.split(".")

    values = []
    for i, step in enumerate(path):
        if isinstance(es_doc, list):
            for subdoc in es_doc:
                values.extend(get_values_from_path(subdoc, path[i + 1 :]))
        elif isinstance(es_doc, dict):
            subdoc = es_doc[step]
            values.extend(get_values_from_path(subdoc, path[i + 1 :]))
        else:
            values.append(es_doc)

    return values


def get_es_doc_count(es_client, index_name, query=None):
    if query is None:
        query = {}
    es_client.indices.refresh(index=index_name)
    return es_client.count(index=index_name, body=query)["count"]


def get_nested_field_by_value_query(field, nested_path, value, not_equals=False):
    """
    Builds query for :field which is underneath nested :nested_path elasticsearch path
    and which has value == :value (value != :value if :not_equals is True)
    """
    if not_equals:
        clause = "must_not"
    else:
        clause = "must"

    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            clause: {
                                "nested": {
                                    "path": nested_path,
                                    "query": {"terms": {field: value}},
                                }
                            }
                        }
                    }
                ]
            }
        }
    }

    return query


def get_non_null_fields(config, blacklist=None):
    """
    Compare the graph index and case_centric mappings to figure out the field
    differences. Given the missing fields, query graph index and check if any
    of the fields have actual values.

    :param config: MI run config
    :param blacklist: a list of fields to ignore in the differences
    :return: list of fields that have values in graph index, but missing not
        defined in case_centric mappings
    """
    if not blacklist:
        # Load default blacklist fields from the config
        blacklist = config.exclude_fields

    es_client = config.source_es

    # Get actual graph index mappings. The object returned by get_mapping has the
    # index name at the top level, but if the index is aliased, it might not match
    # config.graph_case_index, so take whatever the first value is.
    gi_name = config.graph_case_index
    gi_mappings = tuple(es_client.indices.get_mapping(gi_name).values())[0]["mappings"]
    if config.graph_case_doc_type:
        gi_doc_mappings = gi_mappings[config.graph_case_doc_type]
    else:
        gi_doc_mappings = gi_mappings

    # get actual graph index settings
    gi_settings = tuple(es_client.indices.get_settings(gi_name).values())[0]["settings"]

    # Need to create the mappings in gdcmodels format
    gc_mappings = {
        "gdc_from_graph": {
            "case": {"_mapping": gi_doc_mappings},
            "_settings": gi_settings,
        }
    }

    centric_mapper = mapper.ModelMapper("case_centric")
    graph_mapper = mapper.ModelMapper("gdc_from_graph", "case")

    graph_mapper.models = gc_mappings

    # Get exclude fields, some of which can be wildcard
    concretes = []
    wildcards = []
    for f in blacklist:
        if "*" in f:
            # making it regex compatible to filter later
            wildcards.append(f.replace("*.", "*").replace("*", ".*"))
        else:
            concretes.append(f)

    graph_paths = graph_mapper.get_paths()
    centric_paths = centric_mapper.get_paths()

    # filter out explicit fields
    for c in concretes:
        graph_paths = [p for p in graph_paths if not p.startswith(c)]

    # filter out wildcard fields
    for w in wildcards:
        graph_paths = [p for p in graph_paths if not re.match(w, p)]

    missing_paths = set(graph_paths) - set(centric_paths)

    paths_with_data = []

    nested_docs_paths = {
        p.replace("root.", "").replace("properties.", "").replace(".type.nested", "")
        for p in graph_mapper.leaf_paths
        if p.endswith("nested")
    }

    for path in missing_paths:
        nested = None

        parts = path.split(".")
        for i in range(len(parts), 0, -1):
            sub_path = ".".join(parts[:i])
            if sub_path in nested_docs_paths:
                nested = sub_path
                break

        if nested == path:
            # Extend config.case_exclude_fields to exclude nested types
            continue

        exists = {"exists": {"field": path}}
        if nested:
            query = {"nested": {"path": nested, "query": exists}}
        else:
            query = exists

        if (
            get_es_doc_count(
                es_client=es_client, index_name=gi_name, query={"query": query}
            )
            > 0
        ):
            paths_with_data.append(path)

    return paths_with_data


class MappingsLoader:
    """A class for loading the elasticsearch mapping for any given index."""

    def load_mappings(self, index_type: build.IndexType) -> dict:
        """
        Loads the mapping for the given index.

        Args:
            index_type: the index type associated with the elasticsearch mapping to
                load.
        """
        index_name, doc_type = index_type.get_mappings_details()
        model_mapper = mapper.ModelMapper(index_name, doc_type)

        return model_mapper.get_normalized_mappings()["mappings"]


def _flatten_properties(
    properties: Dict[str, Any], excluded_fields: Container[str], path: str = ""
) -> Iterable[str]:
    """
    Flattens the properties found in an elasticsearch mapping into individual fields.

    Args:
        properties: the properties node of an elasticsearch mapping.
        excluded_fields: the fields which should be excluded.
        path: the current path to the given properties. If the top level of the mapping,
            use the given empty string.
    """
    expanded_properties = itertools.starmap(
        lambda name, details: (f"{path}{name}", details), properties.items()
    )
    expanded_properties = filter(
        lambda item: item[0] not in excluded_fields, expanded_properties
    )

    for name, details in expanded_properties:
        if "properties" in details:
            yield from _flatten_properties(
                details["properties"], excluded_fields, f"{name}."
            )
        else:
            yield name


def _format_case_field(field_prefix: str, field: str) -> str:
    """
    Standardizes the format of the selected field. Returning an empty string if the
    field does not contain the given prefix and if it does, then removing the prfix
    from the field.

    Args:
        field_prefix: the prefix found before each field.
        field: the field name being formated.
    """
    if not field_prefix:
        return field

    if field.startswith(field_prefix):
        return re.sub(fr"{field_prefix}\.?", "", field)

    return ""


def _load_case_fields(
    field_prefix: str, excluded_fields: Container[str], mappings: dict
) -> Iterable[str]:
    """
    Loads all case fields from the mapping based on the case field prefix.

    Args:
        field_prefix: the prefix for the case fields found in the mapping.
        excluded_fields: the fields which should be excluded when loading.
        mapping: the elasticsearch mapping from which the fields are being
            loaded.
    """
    properties = mappings["properties"]
    fields = tuple(_flatten_properties(properties, excluded_fields))
    format_field = functools.partial(_format_case_field, field_prefix)

    return filter(None, map(format_field, fields))


class CaseFieldSelector:
    """A class for selecting the case fields in a given elasticsearch index."""

    CASE_PREFIXES = {
        build.IndexType.CASE_CENTRIC: "",
        build.IndexType.CNV_CENTRIC: "occurrence.case",
        build.IndexType.CNV_OCCURRENCE_CENTRIC: "case",
        build.IndexType.SSM_CENTRIC: "occurrence.case",
        build.IndexType.SSM_OCCURRENCE_CENTRIC: "case",
    }

    def __init__(self, mappings_loader: MappingsLoader = MappingsLoader()) -> None:
        self._mapping_loader = mappings_loader

    def _select_fields(
        self, index_type: build.IndexType, excluded_fields: Container[str]
    ) -> Set[str]:
        if index_type not in self.CASE_PREFIXES:
            raise ValueError(f"Index: {index_type} is not supported.")

        field_prefix = self.CASE_PREFIXES[index_type]
        mappings = self._mapping_loader.load_mappings(index_type)
        fields = _load_case_fields(field_prefix, excluded_fields, mappings)

        return frozenset(fields)

    def select_for(
        self,
        *index_types: build.IndexType,
        excluded_fields: Container[str] = (),
        included_fields: Iterable[str] = ("case_id",),
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
        """
        field_sets = (
            self._select_fields(index_type, excluded_fields)
            for index_type in index_types
        )

        return functools.reduce(lambda set0, set1: set0 & set1, field_sets) | frozenset(
            included_fields
        )


def _get_index(config, index_type: build.IndexType) -> str:
    if index_type == build.IndexType.FILE:
        return str(config.graph_file_index)

    if index_type == build.IndexType.CASE:
        return str(config.graph_case_index)

    type_key = index_type.name.lower()

    if type_key in config.indices:
        return config.indices[type_key]

    raise ValueError(f"Index not configured: {index_type.name}")


def _model_mapper_factory(index_type: str) -> mapper.ModelMapper:
    return mapper.ModelMapper(index=index_type)


class DataFrameUtil:
    ES_FORMAT = "org.elasticsearch.spark.sql"

    def __init__(
        self,
        config,
        sql_context: sql.SQLContext,
        es_client: elasticsearch.Elasticsearch,
        model_mapper_factory: Callable[
            [str], mapper.ModelMapper
        ] = _model_mapper_factory,
    ) -> None:
        self._config = config
        self._sql_context = sql_context
        self._es_client = es_client
        self._model_mapper_factory = model_mapper_factory

    def _get_index(self, index_type: build.IndexType) -> str:
        return _get_index(self._config, index_type)

    def read(
        self,
        index_type: build.IndexType,
        include_fields: Union[Iterable[str], bool] = True,
        exclude_fields: Iterable[str] = (),
        include_as_arrays: Iterable[str] = (),
        query: dict = None,
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
        reader = (
            self._sql_context.read.format(self.ES_FORMAT)
            .option("es.read.metadata", read_metadata)
            .option("es.nodes", self._config.source_es_nodes)
            .option("es.net.http.auth.user", self._config.source_es_user)
            .option("es.net.http.auth.pass", self._config.source_es_pass)
            .option("es.net.ssl", self._config.es_use_ssl)
            .option(
                "es.net.ssl.cert.allow.self.signed",
                self._config.disable_es_verify_certs,
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

        return reader.load(self._get_index(index_type))

    get_dataframe = read

    def _create_index(self, index: str, index_type: build.IndexType) -> None:
        """
        Creates the index based on the mapping associated with the given index
        type.

        Args:
            index: the name of the index to be created
            index_type: the index type correlating to the mapping for the new index
        """
        if self._es_client.indices.exists(index=index):
            raise Exception(
                f"Index: {index} already exists. Cannot overwrite existing index."
            )

        index_mapper = self._model_mapper_factory(index_type.name.lower())
        body = index_mapper.get_normalized_mappings()

        self._es_client.indices.create(index=index, body=body)

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
            .option("es.nodes", self._config.es_nodes)
            .option("es.net.http.auth.user", self._config.source_es_user)
            .option("es.net.http.auth.pass", self._config.es_pass)
            .option("es.net.ssl", self._config.es_use_ssl)
            .option(
                "es.net.ssl.cert.allow.self.signed",
                self._config.disable_es_verify_certs,
            )
            .option("es.nodes.wan.only", "true")
            .option("es.nodes.resolve.hostname", "false")
            .option("es.resource.write", index)
            .option("es.http.timeout", "20m")
            .option("es.http.retries", "-1")
            .option("es.batch.write.retry.count", "-1")
            .option("es.batch.write.retry.wait", "10m")
            .option("es.batch.size.bytes", self._config.batch_size_bytes)
            .option("es.batch.size.entries", self._config.batch_size_entries)
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

    def __init__(self, config, spark_context: pyspark.SparkContext) -> None:
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
        query: dict = None,
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
            "es.nodes": self._config.source_es_nodes,
            "es.net.http.auth.user": self._config.source_es_user,
            "es.net.http.auth.pass": self._config.source_es_pass,
            "es.net.ssl": str(self._config.es_use_ssl),
            "es.net.ssl.cert.allow.self.signed": str(
                self._config.disable_es_verify_certs
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
