import json
from typing import Iterable, Optional, Union

import elasticsearch
import pyspark
from elasticsearch import helpers
from normalizer import mapper
from pyspark import sql

from mutation_indexer import constants
from mutation_indexer.configuration import elasticsearch as es_config


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

    def load_mappings(self, index_type: constants.IndexType) -> dict:
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





def _get_index(config: es_config.Elasticsearch, index_type: constants.IndexType) -> str:
    if index_type in config.read.indices:
        return config.read.indices[index_type]

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

    def _get_index(self, index_type: constants.IndexType) -> str:
        return _get_index(self._config, index_type)

    def read(
        self,
        index_type: constants.IndexType,
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

    def _create_index(self, index: str, index_type: constants.IndexType) -> None:
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

        mappings = self._mappings_loader.load_mappings(index_type)

        self._es_client.indices.create(
            index=index, mappings=mappings["mappings"], settings=mappings["settings"]
        )

    def write(
        self, df: sql.DataFrame, index_type: constants.IndexType, id_field: str
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

    def _get_index(self, index_type: constants.IndexType) -> str:
        return _get_index(self._config, index_type)

    def get_rdd(
        self,
        index_type: constants.IndexType,
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
