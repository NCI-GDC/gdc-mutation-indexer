import logging

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,
    lit,
    udf,
)

from exports.builders.utils import melt, remove_columns

logging.basicConfig()


class GisticBuilder(object):
    """
    Read in gistic file and format it for cnv index.
    """
    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self._url = self._get_url(config)

    def build(self):
        """
        Read in gistic file.
        Do necessary massaging
        """
        initial_cnv_df = self._get_initial_cnv()

        # trim gene symbol of last .{dd}
        new_df = self._trim_gene_symbol(initial_cnv_df)

        # how to pass args?
        new_df = remove_columns(new_df, 'Locus ID', 'Cytoband')

        # melt
        new_df = melt(new_df,
                      id_vars=["gene_id"],
                      var_name="aliquot_id",
                      value_name="cnv_change")

        return new_df

    def _get_url(self, config):
        if config.gistic_url is not None:
            return config.gistic_url

        # TODO: what should this actually be?
        return "stuff from indexd most likely"

    def _get_initial_cnv(self, url=None):
        """
        Read gistic into dataframe.
        """

        if url is None:
            if self._url is not None:
                url = self._url
            else:
                self.logger.error("Url not passed, instance _urls not set")
                raise Exception("Url not specified to load CNV")

        # to return
        return_df = None

        try:
            new_df = self._read_gistic(url)

            ###############
            # LOGGING
            self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                           url))
            ###############

            return_df = new_df
        except BaseException as e:
            self.logger.error(e)
            # TODO: reraise?

        assert return_df is not None
        # TODO: ?? self.df = return_df

        return return_df

    def _read_gistic(self, url=None):
        """
        Reads in tab-delimited file into dataframe
        TODO: put in utils
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def _trim_gene_symbol(self, initial_cnv_df):
        """
        Gistic file includes something else
        We want to trim it.
        E.g., ENSG00000008128.21 should be ENSG00000008128
        Unfortunately there is no easy way to do this in place, 
        so we must add the trimmed column and remove the old column.
        """

        def trim_gene_symbol_inner(gene_id):
            period_location = gene_id.rfind('.')
            if period_location != -1:
                gene_id = gene_id[:period_location]

            return gene_id

        trim_gene_symbol_udf = udf(trim_gene_symbol_inner, StringType())
        trimmed_df = initial_cnv_df.withColumn('gene_id',
                                               trim_gene_symbol_udf(
                                                   col('Gene Symbol')
                                               ))

        trimmed_and_deduped_df = trimmed_df.drop('Gene Symbol')

        return trimmed_and_deduped_df

    def _add_ncbi_build(self, initial_cnv_df):

        cnv_df_with_ncbi_build = \
            initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

        return cnv_df_with_ncbi_build

    def _add_gene_level_cn(self, initial_cnv_df):

        cnv_df_with_gene_level_cn = \
            initial_cnv_df.withColumn('gene_level_cn', lit(True))

        return cnv_df_with_gene_level_cn
