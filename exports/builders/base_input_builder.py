import abc
import logging
from typing import Generic, Optional, TypeVar, Union

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types, utils
from typing_extensions import Literal

from exports.configuration.builders import common

TConfig = TypeVar("TConfig", bound=common.Builder)


class BaseInputBuilder(Generic[TConfig], abc.ABC):
    @property
    def config(self) -> TConfig:
        return self._config

    def __init__(
        self, config: TConfig, sqlContext: sql.SQLContext, input_type: str
    ) -> None:
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
        self.sqlContext = sqlContext

        self._config = config

    def build(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        ALWAYS

        #1. read (if applicable)
        #2. build from files
        #3. write (if applicable)
        #4. return (PROFIT)
        """

        # read
        df = self._read()

        # if reading not applicable, build from files
        if df:
            df = self.build_from_cache(df)
        else:
            df = self.build_from_scratch(**kwargs)

        # write
        self._write(df)

        return df.cache() if self.config.is_cached else df

    def build_from_cache(self, df):
        """Perform additional processing on a built DF read from the cache.

        Subclasses can override this to post-process the cached DF. The base
        implementation just returns the cached DF as-is.
        """
        return df

    @abc.abstractmethod
    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        pass

    def _write(self, df: sql.DataFrame) -> None:
        if not self.config.backup.mode.is_write():
            df.write.parquet(path=self.config.backup.path, mode="overwrite")

    def _read(self) -> Optional[sql.DataFrame]:
        """
        Loads previously built and saved input into dataframe
        if we are in read mode
        """
        if not self.config.backup.mode.is_read():
            return None

        df = None
        saved_path = self.config.backup.path

        self.logger.info("Loading file from s3 instead of building")

        try:
            df = self.file_to_df(saved_path, data_format="parquet")
        except IOError:
            self.logger.info(f"File not found in {saved_path}")
        except utils.AnalysisException:
            # TODO: is this the best way to catch this error?
            #   or is checking the path first acceptable?
            self.logger.info(
                "Something went wrong in spark when trying to get existing df from "
                f"path {saved_path}"
            )

        return df

    def file_to_df(
        self,
        url: str,
        data_format: Literal["tsv", "csv", "parquet"] = "tsv",
        header: bool = True,
        schema: Optional[Union[str, types.StructType]] = None,
    ) -> sql.DataFrame:
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
    def add_canonical_transcript_lengths(df: sql.DataFrame) -> sql.DataFrame:
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
