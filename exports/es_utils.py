import enum
import json
import re
from typing import Iterable, Optional, Union

import pyspark
from elasticsearch import helpers
from normalizer import mapper
from pyspark import sql


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


class Index(enum.IntEnum):
    File = 1
    Case = 2


def _get_index_name(config, index: Index) -> str:
    if index == Index.File:
        return str(config.graph_file_index)

    if index == Index.Case:
        return str(config.graph_case_index)

    raise ValueError("Invalid index: {}".format(index))


class DataFrameUtil:
    def __init__(self, config, sql_context: sql.SQLContext) -> None:
        self._config = config
        self._sql_context = sql_context

    def _index_to_str(self, index: Index) -> str:
        return _get_index_name(self._config, index)

    def get_dataframe(
        self,
        index: Index,
        include_fields: Union[Iterable[str], bool] = True,
        exclude_fields: Iterable[str] = (),
        include_as_arrays: Iterable[str] = (),
        query: dict = None,
        read_metadata: bool = False,
    ) -> sql.DataFrame:
        """
        A utility for loading data from ES natively into spark.

        Args:
            index: The index from which the data will be loaded
            include_fields: The fields which will be included when read
            include_as_arrays: The fields which need to be read as arrays and not
                simple types (e.g. field: ["this", "is", "example"])
                NOTE: This does NOT apply to arrays of objects
            query: The query to use in ES to limit the records returned
        """
        reader = (
            self._sql_context.read.format("org.elasticsearch.spark.sql")
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

        return reader.load(self._index_to_str(index))


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

    def _index_to_str(self, index: Index) -> str:
        return _get_index_name(self._config, index)

    def get_rdd(
        self,
        index: Index,
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
            "es.resource": self._index_to_str(index),
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
