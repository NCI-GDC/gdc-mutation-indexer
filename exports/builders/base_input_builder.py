import abc
import logging

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException


class BaseInputBuilder(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, sqlContext, input_type):
        """
        :input_type in ['gistic', 'maf', 'aliquot']
        """
        self.input_type = input_type
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.sqlContext = sqlContext

    @property
    def urls(self):
        config_urls = getattr(self.config, '{}_urls'.format(self.input_type))
        if config_urls is not None:
            return config_urls
        return self.get_urls()

    def build(self):
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
            df = self.build_from_scratch()

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
    def build_from_scratch(self):
        pass

    def get_urls(self):
        """Look up the input URLs if not already given in the config.

        By default, return None to indicate that no URLs were configured. Subclasses
        may override this if appropriate.
        """
        return None

    def write(self, df):
        mode = getattr(self.config, '{}_backup'.format(self.input_type))
        if mode == 'write':
            url = getattr(self.config, '{}_path'.format(self.input_type))
            self.df_to_s3(df, url)

    def df_to_s3(self, df, url):
        """
        Writes the combined input dataframe to s3 in .parquet format
        """
        writer = df.write.format('parquet')
        writer = writer.mode('overwrite')
        writer = writer.options(header='true').save(url)

    def read(self):
        """
        Loads previously built and saved input into dataframe
        if we are in read mode
        """
        # to return
        df = None

        mode = getattr(self.config, '{}_backup'.format(self.input_type))

        if mode == 'read':

            # Load stored built input into dataframe
            saved_path = getattr(self.config, '{}_path'.format(self.input_type))
            self.logger.info('Loading file from s3 instead of building')

            try:
                df = self.file_to_df(saved_path, data_format='parquet')
            except IOError:
                self.logger.info('File not found in {}'.format(saved_path))
            except AnalysisException:
                # TODO: is this the best way to catch this error?
                #   or is checking the path first acceptable?
                self.logger.info('Something went wrong in spark when trying to'
                                 ' get existing df from path '
                                 '{}'.format(saved_path))
            else:
                # Store loaded dataframe count in config
                setattr(self.config,
                        '{}_count'.format(self.input_type),
                        df.count())

        return df

    def file_to_df(self, url, data_format='tsv'):
        """
        Read a single file from the given s3 url and return as dataframe
        """
        if data_format in ['csv', 'tsv']:
            delimiter = '\t' if data_format == 'tsv' else ','
            return self.sqlContext.read.format('com.databricks.spark.csv')\
                       .options(header='true')\
                       .options(comment="#")\
                       .options(delimiter=delimiter)\
                       .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                       .load(url)
        elif data_format == 'parquet':
            return self.sqlContext.read.parquet(url)
        else:
            raise ValueError("Unknown read format: {}".format(data_format))

    @staticmethod
    def add_canonical_transcript_lengths(df):
        """
        Adds canonical_transcript_length{'','cds','genomic'} fields to a dataframe
        """

        def integer_udf(function):
            """ Spark IntegerType udf decorator """
            return udf(function, IntegerType())

        @integer_udf
        def len_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    if 'length' in t:
                        return t['length']
                    else:
                        return None

        @integer_udf
        def len_cds_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    if 'length_cds' in t:
                        return t['length_cds']
                    else:
                        return None

        @integer_udf
        def len_gen_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    return int(t['end']) - int(t['start']) + 1

        df = df.withColumn('canonical_transcript_length',
                           len_udf(df.transcripts))
        df = df.withColumn('canonical_transcript_length_cds',
                           len_cds_udf(df.transcripts))
        df = df.withColumn('canonical_transcript_length_genomic',
                           len_gen_udf(df.transcripts))
        return df
