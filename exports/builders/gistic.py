import logging

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,
    lit,
    udf,
)

from exports.builders.df_builders import (
    get_gene_df,
)

from exports.builders.utils import (
    melt_df,
    remove_columns,
    uuid5_col,
)

logging.basicConfig()


class GisticBuilder(object):

    # TEMP
    old_to_new = {'gene_chromosome': 'chromosome',
                  'gene_start': 'start_position',
                  'gene_end': 'end_position'}

    """
    Read in gistic file and format it for cnv index.
    """
    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self._url = self._get_url(config)

    def build(self, maf_df):
        """
        Read in gistic file.
        Do necessary massaging
        """
        cnv_df = self._get_initial_cnv()

        cnv_df = self._trim_gene_symbol(cnv_df)

        cnv_df = remove_columns(cnv_df, 'Locus ID', 'Cytoband')

        cnv_df = melt_df(cnv_df,
                         id_vars=["gene_id"],
                         var_name="aliquot_id",
                         value_name="cnv_change")

        # add gene information
        cnv_df = self._add_gene_information(cnv_df, maf_df)

        # TODO: add case id

        # drop 0 entries
        # convert to string

        return cnv_df

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

            self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                           url))

            return_df = new_df
        except BaseException as e:
            self.logger.error(e)

        assert return_df is not None

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

    def _add_gene_information(self, initial_cnv_df, maf_df):

        # join to gene df on trimmed gene symbol = gene_id
        new_df = self._join_to_gene(initial_cnv_df, maf_df)

        # gene information is required to create cnv_id
        new_df = self._add_id(new_df)

        return new_df

    def _join_to_gene(self, initial_cnv_df, maf_df):
        """
        Get the other gene information
        """
        gene_df = get_gene_df(maf_df, index_name='gene_centric',
                              unique_fields=['gene_id'])

        self.logger.info('Joining gene with gistic [inner, "gene_id"]')
        df = (
            gene_df.join(initial_cnv_df,
                         gene_df.gene_id == initial_cnv_df.gene_id,
                         'inner')
                   .drop(initial_cnv_df.gene_id)
        )

        # rename columns from gene names to cnv names
        for old, new in GisticBuilder.old_to_new.items():
            df = df.withColumnRenamed(old, new)

        return df

    def _add_id(self, initial_cnv_df):
        """
        Business key: chromosome, start_position, end_position, cnv_change
        """

        cnv_df_with_id = initial_cnv_df.withColumn('cnv_id', uuid5_col(
            col('chromosome'),
            col('start_position'),
            col('end_position'),
            col('cnv_change')
        ))

        return cnv_df_with_id
