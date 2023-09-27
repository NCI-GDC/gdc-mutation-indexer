import abc
import json
import logging

from normalizer.mapper import ModelMapper
from pyspark import sql
from typing_extensions import Self

from mutation_indexer.builders import utils
from mutation_indexer.constants import app

logging.basicConfig(format=app.LOG_FORMAT)


class BaseBuilder:
    """
    BaseBuilder contains the structure necessary for a Builder object.
    """

    __metaclass__ = abc.ABCMeta

    index_name = None
    id_field = None
    settings = None

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug

    @abc.abstractmethod
    def build(self, **kwargs: sql.DataFrame) -> Self:
        """
        Contains the ETL logic to construct a spark dataframe of
        the same structure as the required output index.
        """
        pass

    def load(self):
        """
        Responsible for loading the dataframe resulting from :func:`build`
        into a destination, usually Elasticsearch.
        """
        index = self.config.indices[self.index_name]

        index_mapper = ModelMapper(self.index_name)
        if self.config.skip_normalization:
            index_body = index_mapper.index_settings
        else:
            index_body = index_mapper.get_normalized_mappings()
        index_body = json.dumps(index_body)

        self.log("Creating {} index".format(index))
        response = self.config.es.indices.create(index=index, body=index_body)
        self.log(response)

        self.log("Repartitioning {}".format(self.index_name))
        df = getattr(self, self.index_name).repartition(
            self.config.df_repartition, self.id_field
        )

        self.log("Exporting {} index to {}".format(self.index_name, index))
        df.coalesce(self.config.df_coalesce).write.format(
            "org.elasticsearch.spark.sql"
        ).option("es.nodes", self.config.es_nodes).option(
            "es.net.http.auth.user", self.config.source_es_user
        ).option(
            "es.net.http.auth.pass", self.config.es_pass
        ).option(
            "es.net.ssl", self.config.es_use_ssl
        ).option(
            "es.net.ssl.cert.allow.self.signed", self.config.disable_es_verify_certs
        ).option(
            "es.nodes.wan.only", "true"
        ).option(
            "es.nodes.resolve.hostname", "false"
        ).option(
            "es.resource.write", index
        ).option(
            "es.http.timeout", "20m"
        ).option(
            "es.http.retries", "-1"
        ).option(
            "es.batch.write.retry.count", "-1"
        ).option(
            "es.batch.write.retry.wait", "10m"
        ).option(
            "es.batch.size.bytes", self.config.batch_size_bytes
        ).option(
            "es.batch.size.entries", self.config.batch_size_entries
        ).option(
            "es.batch.write.refresh", False
        ).option(
            "es.mapping.id", self.id_field
        ).save(
            index
        )
        self.log("Finished exporting {} index to {}".format(self.index_name, index))

        df.unpersist()

    def truncate_df_at_percentile(
        self, df_to_truncate, field, percentile_threshold,
    ):
        """
        Truncates df_to_truncate to remove rows
        where field > percentile_threshold
        """
        return utils.filter_arrays_by_relative_size(
            df_to_truncate, field, percentile_threshold
        )

    def load_raw(self, path=None):
        """
        Loads the computed index's dataframe, if it exists, and return it,
        returns None it does not
        """
        if path is None:
            path = self.config.get_raw_output_path(self.index_name)
        try:
            self.logger.info("Using existing index from {}".format(path))
            df = self.sqlContext.read.load(path)
            return df
        except Exception:
            self.logger.info("Couldn't find file at {}".format(path))
            return None

    def write(self, path=None):
        """
        Writes the built dataframe to a json file at path
        """
        if not self.config.output_raw == "write":
            self.logger.info("Will not write raw output to s3")
            return

        if path is None:
            path = self.config.get_raw_output_path(self.index_name)

        df = getattr(self, self.index_name, None)
        assert df is not None, "Builder does not have index_name attribute"

        # Repartition by the id into number of partitions specified in config
        id_field = getattr(self, self.id_field, None)
        if id_field:
            df = df.repartition(self.config.df_repartition, id_field).write
        else:
            df = df.repartition(self.config.df_repartition).write
            df = df.mode("overwrite")
        self.logger.info("Saving {} to {}".format(self.index_name, path))
        df.json(path)

    def log(self, string):
        """
        Handles Builder logging.
        """
        self.logger.info(string)

    def log_count(self, dataframe):
        """
        Logs dataframe count if in Debug mode
        """
        if self.debug:
            self.log("Count: {}".format(dataframe.count()))
