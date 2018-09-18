from pyspark.sql.functions import (
    lit, collect_set, col, udf,
)
from pyspark.sql.types import StringType
from utils import standardize_schema, get_case_ids_from_source_es
import logging
logging.basicConfig()


class CaseBuilder(object):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.maf_urls = config.maf_urls

    def build(self, maf_df, gistic_df=None):
        """
        Builds Case dataframe
        """
        df = self.load_into_df(maf_df, gistic_df)

        # Select only columns that are in case mapping:
        df = standardize_schema(df, 'case_centric', 'case')

        return df

    def load_into_df(self, maf_df, gistic_df=None):
        """
        Loads case docs from the gdc_from_graph index into a dataframe
        """
        source = '{}/{}'.format(self.config.graph_index,
                                self.config.graph_document)

        # Load all cases from graph_index
        df = (
            self.sqlContext.read.format("es")
            .option('es.nodes', '{}:{}'.format(self.config.source_es_host,
                                               self.config.source_es_port))
            .option('es.net.http.auth.user', self.config.source_es_user)
            .option('es.net.http.auth.pass', self.config.source_es_pass)
            .option('es.nodes.wan.only', 'true')
            .option('es.nodes.resolve.hostname', 'false')
            .option('es.read.field.exclude', ','.join(
                    self.config.case_exclude_fields))
            .option('es.resource.read', source)
            .load(source)
        )

        maf_and_gistic_df = self.populate_available_variation_data(maf_df,
                                                                   gistic_df)

        df = df.join(maf_and_gistic_df, on=['case_id'], how='left')

        self.logger.info('Repartitioning case dataframe')
        df = df.repartition(self.config.repartition, 'case_id')

        if self.config.cache_dataframes['cases']:
            self.logger.info('Caching repartitioned case dataframe')
            df.cache().count()

        return df

    def populate_available_variation_data(self, maf_df, gistic_df=None):
        """
        This function calculates the value of the column
        "available_variation_data."

        We retrieve a set of cases from graph_index
        and add "ssm" if that case id is present in the maf_df,
        "cnv" if that case id is present in the gistic_df,
        ["ssm", "cnv"] if both, and [] if neither.
        """

        avd = 'available_variation_data'

        # Get set of "tested cases" from maf_df
        maf_data = (maf_df.select('case_id', avd)
                          .dropDuplicates(
                              subset=['case_id',
                                      avd]))

        maf_data = (maf_data.withColumn('temp', lit('ssm'))).drop(avd)

        # Get set of cnv cases from gistic_df
        if gistic_df:
            gistic_data = (gistic_df.select('case_id', 'cnv_id'))
            avd_udf = udf(lambda x: None if x is None else 'cnv', StringType())
            gistic_data = (
                gistic_data.withColumn('temp',
                                       avd_udf(col('cnv_id')))).drop('cnv_id')

            # Stack
            maf_and_gistic_data = maf_data.union(gistic_data)
        else:
            maf_and_gistic_data = maf_data

        # Get all the cases that have been tested
        # (from aliquots in maf_df headers)
        cases_to_keep = get_case_ids_from_source_es(
            self.config, self.sqlContext, self.maf_urls
        )

        # Add empty rows to input_data corresponding to "empty cases"
        df = maf_and_gistic_data.join(cases_to_keep,
                                      on=['case_id'], how='right')
        # Finally, group by case
        df = (df.groupby('case_id').agg(collect_set('temp').alias(avd)))

        return df
