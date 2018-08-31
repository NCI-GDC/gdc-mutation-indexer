import logging

from elasticsearch import Elasticsearch

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,
    lit,
    udf,
)

from exports.builders.df_builders import (
    get_gene_df,
)
from exports.builders.gene_model import GeneModelBuilder
from exports.builders.utils import (
    melt_df,
    uuid5_col,
    remove_columns,
    iterate_es_results,
    map_create_column,
    uuid5_col,
)

logging.basicConfig()


class GisticBuilder(object):

    gene_to_cnv_col_names = {'gene_chromosome': 'chromosome',
                             'gene_start': 'start_position',
                             'gene_end': 'end_position'}

    """
    Read in gistic file and format it for cnv index.

    NOTE: For gistic file, these positions exactly match gene_start and gene_end
    (c) Kyle Hernandez

    NOTE: gene_level_cn = True is a placeholder for future use (c) Junjun

    NOTE: ncbi_build = 'GRCh38' - constant value, same as in ssm branch (c) Zhenyu

    TODO: Create abstract base class for this and MAFBuilder enforcing .build(), .combine(), .read()

    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.urls = config.get_gistic_urls()
        self.es = Elasticsearch(config.es_host,
                                port=config.es_port,
                                http_auth=(config.es_user,
                                           config.es_pass))

    def build(self):
        """
        Read, combine and transform gistic files

        Returns gistic_df
        """
        gistic_df = self.combine()

        # add gene information
        gistic_df = self._add_gene_information(gistic_df)

        # add cnv_id
        gistic_df = self._add_cnv_id(gistic_df)

        # drop entries with cnv_change == 0 and cast cnv_change to string
        gistic_df = self._cnv_change_to_string_and_drop_zero(gistic_df)

        # transform aliquot_id column to case_id
        gistic_df = self._aliquot_id_to_case_id(gistic_df)

        return gistic_df

    def combine(self, urls=None):
        """
        Combines data frames from a list of urls
        """
        if urls is None and self.urls is not None:
            urls = self.urls
        elif urls is None and self.urls is None:
            self.logger.error('Urls not passed and get_urls() not yet called')
            raise Exception

        gistic_df = None
        for url in urls:
            try:
                new_df = self.read(url)
                self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                               url))

                # prepare to melt
                new_df = self._trim_gene_symbol(new_df)

                new_df = remove_columns(new_df, 'Locus ID', 'Cytoband')

                # melt dataframe (opposite of pivoting)
                # required to get dfs with the same number of columns
                # so we can union them together
                new_df = melt_df(new_df,
                                 id_vars=["gene_id"],
                                 var_name="aliquot_id",
                                 value_name="cnv_change")

                if gistic_df is None:
                    gistic_df = new_df
                else:
                    gistic_df = gistic_df.unionAll(new_df)
            except BaseException as e:
                self.logger.error(e)

        return gistic_df

    def read(self, url):
        """
        Reads gistic file into dataframe
        TODO: put in utils as read_tsv and import to use here
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def _add_cnv_id(self, gistic_df):
        """
        cnv_id ~ (chromosome, gene_start, gene_end, cnv_change)
        """

        # NOTE: start_position and end_position are matching with
        #       gene_start and gene_end in gistic context (c) Zhenyu and Kyle
        gistic_df = gistic_df.withColumn('cnv_id', uuid5_col(
            col('chromosome'),
            col('start_position'),
            col('end_position'),
            col('cnv_change')
        ))
        return gistic_df

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

    def _add_gene_information(self, gistic_df):

        # get gene_df
        gm_df = GeneModelBuilder(self.config, self.sqlContext).build()

        # add gene info to gistic_df
        new_df = gistic_df.join(gm_df, gistic_df.gene_id == gm_df._gene_id)

        # rename columns from gene names to cnv names
        for old, new in GisticBuilder.gene_to_cnv_col_names.items():
            new_df = new_df.withColumnRenamed(old, new)

        # gene information is required to create cnv_id
        new_df = self._add_ncbi_build(new_df)

        new_df = self._add_gene_level_cn(new_df)

        return new_df

    def _add_ncbi_build(self, initial_cnv_df):

        cnv_df_with_ncbi_build = \
            initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

        return cnv_df_with_ncbi_build

    def _add_gene_level_cn(self, initial_cnv_df):

        cnv_df_with_gene_level_cn = \
            initial_cnv_df.withColumn('gene_level_cn', lit(True))

        return cnv_df_with_gene_level_cn

    def _aliquot_id_to_case_id(self, df):
        """
        Looks up aliquot_id to case_id mapping from gdc_from_graph.case
        and adds case_id column accordingly
        """
        # get list of aliquot_ids to transform
        aliquot_ids = df.select('aliquot_id').rdd.map(lambda x: x[0]).collect()

        # query all case_documents that have relevant aliquots attached
        query = {
            "query": {
                "constant_score": {
                    "filter": {
                        "terms": {
                            "aliquot_ids": aliquot_ids
                        }
                    }
                }
            },
            '_source': ['aliquot_ids']
        }

        relevant_cases = iterate_es_results(self.es, self.config.graph_index, 'case', query=query)

        # build aliquot to case mapping
        aliquot_to_case_map = {}
        for case in relevant_cases:
            for aliquot_id in case['_source']['aliquot_ids']:
                aliquot_to_case_map[aliquot_id] = case['_id']

        # create case_id column based on aliquot_id column, drop aliquot_id
        def map_aliquot_to_case(aliquot):
            return aliquot_to_case_map[aliquot]

        df = map_create_column(df, map_aliquot_to_case, 'aliquot_id', 'case_id')

        return df

    def _cnv_change_to_string_and_drop_zero(self, df):
        """
        We map input cnv_change str number codes to str interpretations
        (i.e., '2' becomes 'Amplification').
        We map '0' to None and drop rows that have 'cnv_change' == None.
        """
        # rename col to replace
        df = df.withColumnRenamed('cnv_change', 'cnv_change_init')

        cnv_change_mapping = {'-2': 'Deep Loss',
                              '-1': 'Shallow Loss',
                              '0': None,
                              '1': 'Gain',
                              '2': 'Amplification'}

        def stringify_cnv_change_inner(int_cnv):
            try:
                return cnv_change_mapping[int_cnv]
            except KeyError as e:
                raise e

        stringify_cnv_change_udf = udf(stringify_cnv_change_inner,
                                       StringType())
        new_df = df.withColumn('cnv_change',
                               stringify_cnv_change_udf(
                                   col('cnv_change_init')))

        new_df = new_df.drop('cnv_change_init')

        # drop 0/None
        new_df = new_df.na.drop(subset=['cnv_change'])

        return new_df

