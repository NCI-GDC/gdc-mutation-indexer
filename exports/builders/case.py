import logging

from pyspark.sql.functions import (
    col,
    collect_set,
    lit,
    udf,
)
from pyspark.sql.types import StringType, ArrayType

from utils import (
    get_case_ids_from_source_es,
    remove_columns,
    standardize_schema,
)

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

        self.logger.info('Exclude fields: {}'.format(self.config.exclude_fields))

        # Load all cases from graph_index
        df = (
            self.sqlContext.read.format("es")
            .option('es.nodes', '{}:{}'.format(self.config.source_es_host,
                                               self.config.source_es_port))
            .option('es.net.http.auth.user', self.config.source_es_user)
            .option('es.net.http.auth.pass', self.config.source_es_pass)
            .option('es.nodes.wan.only', 'true')
            .option('es.nodes.resolve.hostname', 'false')
            .option('es.read.field.exclude', ','.join(self.config.exclude_fields))
            .option('es.resource.read', source)
            .load(source)
        )

        # Get all the cases that have been tested for ssm
        # (from aliquots in maf_df headers)
        all_maf_cases = get_case_ids_from_source_es(self.config,
                                                    self.sqlContext)

        maf_and_gistic_df = self.populate_available_variation_data(maf_df,
                                                                   all_maf_cases,
                                                                   gistic_df)

        df = df.join(maf_and_gistic_df, on=['case_id'], how='left')

        acl_df = self.populate_ssm_acl(maf_df, all_maf_cases)

        df = df.join(acl_df, on=['case_id'], how='left')

        self.logger.info('Repartitioning case dataframe')
        df = df.repartition(self.config.df_repartition, 'case_id')

        if self.config.cache_dataframes['cases']:
            self.logger.info('Caching repartitioned case dataframe')
            df.cache().count()

        return df

    def populate_available_variation_data(self, maf_df, all_maf_cases, gistic_df):
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

        # Add empty rows to input_data corresponding to "empty cases"
        maf_data = (all_maf_cases.join(maf_data,
                                       on=['case_id'],
                                       how='left')).drop('case_acl')

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

    def populate_ssm_acl(self, maf_df, all_maf_cases):
        """
        We use the observation level case_acl calculated in all_maf_cases.
        If no observation level ssm acl exists,
            case level ssm_acl will be populated according to SSM access policy
            assuming the case had ssm data.
        """
        # Get set of "tested cases" from maf_df
        maf_data = (maf_df.select('case_id', 'acl')
                          .dropDuplicates(subset=['case_id']))

        # Add empty rows to input_data corresponding to "empty cases"
        maf_data = all_maf_cases.join(maf_data,
                                      on=['case_id'], how='left')

        # Merge maf-level 'acl' with case-level acl
        # I.e., use case-level acl where it exists, otherwise ssm-level
        ssm_acl_udf = udf(lambda x, y:
                          x if x is not None else y,
                          ArrayType(StringType()))

        ssm_acl_df = maf_data.withColumn('ssm_acl',
                                         ssm_acl_udf(col('case_acl'),
                                                     col('acl')))
        # drop the input acl columns
        ssm_acl_df = remove_columns(ssm_acl_df, 'acl', 'case_acl')

        return ssm_acl_df
