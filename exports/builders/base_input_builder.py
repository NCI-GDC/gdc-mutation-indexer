from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import logging


class BaseInputBuilder(object):

    def __init__(self, config, sqlContext, input_type):
        """
        :input_type in ['gistic', 'maf']
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

    def build():
        raise NotImplementedError

    def combine():
        raise NotImplementedError

    def s3_to_df(self, url, data_format='csv'):
        """
        Read a single file from the given s3 url and return as dataframe
        """
        if data_format == 'csv':
            return self.sqlContext.read.format('com.databricks.spark.csv')\
                       .options(header='true')\
                       .options(comment="#")\
                       .options(delimiter='\t')\
                       .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                       .load(url)
        elif data_format == 'parquet':
            return self.sqlContext.read.parquet(url)
        else:
            raise ValueError("Unknown read format: {}".format(data_format))

    def df_to_s3(self, df, url, overwrite=False):
        """
        Writes the combined input dataframe to s3 in .parquet format
        """
        writer = df.write.format('parquet')
        if overwrite:
            writer = writer.mode('overwrite')
        writer = writer.options(header='true').save(url)

    def get_existing(self):
        """
        Loads previously built and saved input into dataframe
        """
        # Load stored built input into dataframe
        saved_path = getattr(self.config, '{}_path'.format(self.input_type))
        self.logger.info('Loading file from s3 instead of building')

        try:
            df = self.s3_to_df(saved_path, data_format='parquet')
        except IOError:
            self.logger.info('File not found in {}'.format(saved_path))

        # Store loaded dataframe count in config
        setattr(self.config, '{}_count'.format(self.input_type), df.count())
        return df

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
