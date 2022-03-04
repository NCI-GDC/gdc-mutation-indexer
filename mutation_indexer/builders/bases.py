import abc
import copy
import json
import logging

from normalizer import mapper
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql import utils as sql_utils

import mutation_indexer.config as config
from mutation_indexer.builders import utils

logging.basicConfig(format=config.LOG_FORMAT)


def get_all_boolean_paths(mapping):
    """Find all the boolean field in mapping and return the paths

    Args:
        mapping: dict of mapping types

    Returns:
        list of path, each path is a list of field names
    """
    res = []

    def helper(node, path=None):
        if path is None:
            path = []

        for key, value in node["properties"].items():
            if value.get("type") == "boolean":
                res.append(path + [key])
            elif "properties" in value:
                helper(value, path + [key])

    helper(mapping)
    return res


def cast_booleans(df, mapping):
    """Ensure all the boolean fields in data frame are booleans before save to ES

    Args:
        df: pyspark dataframe to cast boolean
        mapping: Dict of mapping types

    Returns:
        pyspark dataframe with boolean field casted
    """
    paths = get_all_boolean_paths(mapping)
    schema = copy.deepcopy(df.schema)
    for path in paths:
        field = None
        for node in path:
            if field is None:
                field = schema[node]
            elif isinstance(field.dataType, types.StructType):
                field = field.dataType[node]
            elif isinstance(field.dataType, types.ArrayType):
                field = field.dataType.elementType[node]
            elif isinstance(field.dataType, types.MapType):
                raise ValueError("Unsupported Property Type")
        field.dataType = types.BooleanType()

    select_expr = [df[f.name].cast(f.dataType) for f in schema.fields]
    return df.select(*select_expr)


class BaseBuilder(abc.ABC):
    """
    BaseBuilder contains the structure necessary for a Builder object.
    """

    index_name = None
    id_field = None
    settings = None

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug

    @abc.abstractmethod
    def build(self, *args, **kwargs):
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

        index_mapper = mapper.ModelMapper(self.index_name)
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

        df = cast_booleans(df, index_mapper.mapping)
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
        self,
        df_to_truncate,
        field,
        percentile_threshold,
        df_for_percentile_calculation=None,
    ):
        """
        Truncates df_to_truncate to remove rows
        where field > percentile_threshold
        """

        if percentile_threshold < 100:
            self.log("Calculating number of {}".format(field))

            count_col_name = "{}_count".format(field.replace(".", "_"))
            df_to_truncate = df_to_truncate.withColumn(
                count_col_name, F.size(F.col(field))
            )
            if df_for_percentile_calculation is None:
                df_for_percentile_calculation = df_to_truncate
            else:
                df_for_percentile_calculation = (
                    df_for_percentile_calculation.withColumn(
                        count_col_name, F.size(F.col(field))
                    )
                )

            self.log("Calculating {} percentile".format(percentile_threshold))
            threshold = utils.percentile(
                [
                    int(r[count_col_name])
                    for r in df_for_percentile_calculation.select(
                        count_col_name
                    ).collect()
                ],
                percentile_threshold,
            )

            self.log(
                "Truncating dataframe"
                "(removing rows where number of "
                "{} > {})".format(field, threshold)
            )

            df_to_truncate = df_to_truncate.filter(
                "{} <= {}".format(count_col_name, threshold)
            ).drop(count_col_name)

        return df_to_truncate

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


class BaseInputBuilder(abc.ABC):
    def __init__(self, config, sqlContext, input_type):
        """

        Args:
            config(BaseConfig): a config instance
            sqlContext: spark sql context instance
            input_type(str): a string that uniquely represents an output data
                frame type. Can be one of: (
                    'gistic',
                    'maf',
                    'aliquot',
                    'gene_expression_values',
                    'gene_expression_cases',
                )
        """
        self.input_type = input_type
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.sqlContext = sqlContext

    @property
    def urls(self):
        config_urls = getattr(self.config, "{}_urls".format(self.input_type), None)
        if config_urls is not None:
            return config_urls
        return self.get_urls()

    def build(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        ALWAYS

        #1. read (if applicable)
        #2. build from files
        #3. write (if applicable)
        #4. return (PROFIT)
        """

        # read
        df = self.read()

        # if reading not applicable, build from files
        if df:
            df = self.build_from_cache(df)
        else:
            df = self.build_from_scratch(**kwargs)

        # write
        self.write(df)

        return df

    def build_from_cache(self, df):
        """Perform additional processing on a built DF read from the cache.

        Subclasses can override this to post-process the cached DF. The base
        implementation just returns the cached DF as-is.
        """
        return df

    @abc.abstractmethod
    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        pass

    def get_urls(self):
        """Look up the input URLs if not already given in the config.

        By default, return None to indicate that no URLs were configured. Subclasses
        may override this if appropriate.
        """
        return None

    def write(self, df):
        mode = getattr(self.config, "{}_backup".format(self.input_type))
        if mode == "write":
            url = getattr(self.config, "{}_path".format(self.input_type))
            self.df_to_s3(df, url)

    def df_to_s3(self, df, url):
        """
        Writes the combined input dataframe to s3 in .parquet format
        """
        writer = df.write.format("parquet")
        writer = writer.mode("overwrite")
        writer = writer.options(header="true").save(url)

    def read(self):
        """
        Loads previously built and saved input into dataframe
        if we are in read mode
        """
        # to return
        df = None

        mode = getattr(self.config, "{}_backup".format(self.input_type))

        if mode == "read":

            # Load stored built input into dataframe
            saved_path = getattr(self.config, "{}_path".format(self.input_type))
            self.logger.info("Loading file from s3 instead of building")

            try:
                df = self.file_to_df(saved_path, data_format="parquet")
            except IOError:
                self.logger.info("File not found in {}".format(saved_path))
            except sql_utils.AnalysisException:
                # TODO: is this the best way to catch this error?
                #   or is checking the path first acceptable?
                self.logger.info(
                    "Something went wrong in spark when trying to"
                    " get existing df from path "
                    "{}".format(saved_path)
                )
            else:
                # Store loaded dataframe count in config
                setattr(self.config, "{}_count".format(self.input_type), df.count())

        return df

    def file_to_df(self, url, data_format="tsv", header=True, schema=None):
        """
        Read a single file from the given s3 url and return as dataframe
        """
        if data_format in ["csv", "tsv"]:
            delimiter = "\t" if data_format == "tsv" else ","
            return (
                self.sqlContext.read.format("com.databricks.spark.csv")
                .options(comment="#")
                .options(delimiter=delimiter)
                .options(codec="org.apache.hadoop.io.compress.GzipCodec")
                .load(url, header=header, schema=schema)
            )
        elif data_format == "parquet":
            return self.sqlContext.read.parquet(url)
        else:
            raise ValueError("Unknown read format: {}".format(data_format))

    @staticmethod
    def add_canonical_transcript_lengths(df):
        """
        Adds canonical_transcript_length{'','cds','genomic'} fields to a dataframe
        """

        def integer_udf(function):
            """Spark IntegerType udf decorator"""
            return F.udf(function, types.IntegerType())

        @integer_udf
        def len_udf(transcripts):
            for t in transcripts:
                if t["is_canonical"]:
                    if "length" in t:
                        return t["length"]
                    else:
                        return None

        @integer_udf
        def len_cds_udf(transcripts):
            for t in transcripts:
                if t["is_canonical"]:
                    if "length_cds" in t:
                        return t["length_cds"]
                    else:
                        return None

        @integer_udf
        def len_gen_udf(transcripts):
            for t in transcripts:
                if t["is_canonical"]:
                    return int(t["end"]) - int(t["start"]) + 1

        df = df.withColumn("canonical_transcript_length", len_udf(df.transcripts))
        df = df.withColumn(
            "canonical_transcript_length_cds", len_cds_udf(df.transcripts)
        )
        df = df.withColumn(
            "canonical_transcript_length_genomic", len_gen_udf(df.transcripts)
        )
        return df
