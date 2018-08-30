import logging

from elasticsearch import Elasticsearch

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,
    lit,
    udf,
)

from exports.builders.utils import (
    melt_df,
    remove_columns,
    iterate_es_results,
    map_create_column,
)


logging.basicConfig()


class GisticBuilder(object):
    """
    Read in gistic file and format it for cnv index.

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
        Read in gistic file.
        Do necessary massaging
        """
        cnv_df = self.combine()

        cnv_df = self._trim_gene_symbol(cnv_df)

        cnv_df = remove_columns(cnv_df, 'Locus ID', 'Cytoband')

        # melt dataframe (opposite of pivoting)
        cnv_df = melt_df(cnv_df,
                         id_vars=["gene_id"],
                         var_name="aliquot_id",
                         value_name="cnv_change")

        # transform aliquot_id column to case_id
        cnv_df = self._aliquot_id_to_case_id(cnv_df)

        return cnv_df

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
                self.logger.info('Read {} rows from {}'.format(new_df.count(), url))
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

    def _aliquot_id_to_case_id(self, df):
        """
        Looks up aliquot_id to case_id mapping from gdc_from_graph.case
        and renames gistic dataframe columns respectively
        """
        # get list of aliquot_ids to transform
        aliquot_ids = df.select('aliquot_id').rdd.map(lambda x: x[0]).collect()

        # query all case_documents that have relevant aliquots attached
        query = {
            "query" : {
                "constant_score" : {
                    "filter" : {
                        "terms" : {
                            "aliquot_ids" : aliquot_ids
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
        df = df.drop('aliquot_id')

        return df

    def _add_ncbi_build(self, initial_cnv_df):

        cnv_df_with_ncbi_build = \
            initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

        return cnv_df_with_ncbi_build

    def _add_gene_level_cn(self, initial_cnv_df):

        cnv_df_with_gene_level_cn = \
            initial_cnv_df.withColumn('gene_level_cn', lit(True))

        return cnv_df_with_gene_level_cn
