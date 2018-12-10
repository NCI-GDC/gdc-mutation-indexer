from pyspark.sql.functions import (
    lit, collect_set, col, udf,
)
from pyspark.sql.types import StringType
from utils import standardize_schema, get_case_ids_from_source_es
import logging

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CaseBuilder(object):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.maf_urls = config.maf_urls

    def build(self, maf_df, gistic_df):
        """
        Builds Case dataframe
        """
        df = self.load_into_df(maf_df, gistic_df)

        # Select only columns that are in case mapping:
        df = standardize_schema(df, 'case_centric', 'case')

        return df

    def load_into_df(self, maf_df, gistic_df):
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
            .option('es.read.field.exclude',
                    ','.join(self.config.case_exclude_fields))
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

    def populate_available_variation_data(self, maf_df, gistic_df):
        """
        This function calculates the value of the column
        "available_variation_data."

        We retrieve a set of cases from graph_index
        and add "ssm" for both those cases and the cases in the maf_df,
        "cnv" if that case id is present in the gistic_df,
        ["ssm", "cnv"] if both.
        """

        avd = 'available_variation_data'

        # Get set of "tested cases" from maf_df
        maf_data = (maf_df.select('case_id', avd)
                          .dropDuplicates(
                              subset=['case_id',
                                      avd]))

        # Get all the cases that have been tested for ssm
        # (from aliquots in maf_df headers)
        all_maf_cases = get_case_ids_from_source_es(
            self.config, self.sqlContext, self.maf_urls
        )

        # Add empty rows to input_data corresponding to "empty cases"
        maf_data = all_maf_cases.join(maf_data,
                                      on=['case_id'], how='left')

        # the original maf_data is in array form ['ssm'] and we need 'ssm'
        maf_data = maf_data.drop(avd)
        # Set all cases in maf_data to "tested"
        # i.e., 'available_variation_data' == 'ssm'
        maf_data = (maf_data.withColumn(avd, lit('ssm')))

        # Stack with gistic data
        maf_and_gistic_data = maf_data.union((
                                gistic_df.select('case_id', avd)))

        # Finally, group by case
        maf_and_gistic_data = (maf_and_gistic_data
                               .groupby('case_id')
                               .agg(collect_set(avd).alias(avd)))

        return maf_and_gistic_data

