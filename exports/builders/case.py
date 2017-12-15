from pyspark.sql.functions import lit, collect_list
from utils import select_mapping, get_case_ids_from_source_es
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
        self.urls = config.maf_urls

    def build(self, maf_df):
        """
        Builds Case dataframe
        """
        df = self.load(maf_df)

        # Select only columns that are in case mapping:
        case_mapping = select_mapping('case_centric', 'case')['properties']
        columns_to_keep = [c for c in df.columns if c in case_mapping.keys()]
        df = df.select(*columns_to_keep)

        return df

    def load(self, maf_df):
        """
        Loads case docs from the gdc_from_graph index into a dataframe
        """
        source = '{}/{}'.format(self.config.graph_index, self.config.graph_document)

        # Load all cases from graph_index
        df = (
            self.sqlContext.read.format("es")
            .option('es.nodes', '{}:{}'.format(self.config.source_es_host,
                                               self.config.source_es_port))
            .option('es.net.http.auth.user', self.config.source_es_user)
            .option('es.net.http.auth.pass', self.config.source_es_pass)
            .option('es.nodes.wan.only', 'true')
            .option('es.nodes.resolve.hostname', 'false')
            .option('es.read.field.exclude', ','.join(self.config.case_exclude_fields))
            .option('es.resource.read', source)
            .load(source)
        )

        # Get set of "tested cases" from maf
        # NOTE: available_variation_data will be equal 'ssm' for cases that are "tested"
        # and will be empty for "empty cases"
        maf_columns = ['available_variation_data']
        maf_data = (maf_df.select('case_id', *maf_columns)
                          .dropDuplicates(subset=['case_id'] + maf_columns))

        # Get all the cases that have been tested (from aliquots in maf headers)
        cases_to_keep = get_case_ids_from_source_es(
            self.config, self.sqlContext, self.urls
        )
        # Add empty rows to maf_data corresponding to "empty cases"
        maf_data = maf_data.join(cases_to_keep, on=['case_id'], how='right')

        # Set all cases in maf_data to "tested", i.e. 'available_variation_data' == 'ssm'
        maf_data = (
            maf_data.withColumn('t', lit('ssm'))
                    .groupby('case_id')
                    .agg(
                        collect_list('t').alias('available_variation_data')
                    )
        )

        df = df.join(maf_data, on=['case_id'], how='left')

        self.logger.info('Repartitioning case dataframe')
        df = df.repartition(self.config.repartition, 'case_id')

        if self.config.cache_dataframes['cases']:
            self.logger.info('Caching repartitioned case dataframe')
            df.cache().count()
        return df

