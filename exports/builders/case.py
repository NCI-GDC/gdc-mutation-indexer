from utils import select_mapping
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

        df = self.sqlContext.read.format("es")\
            .option('es.nodes', '{}:{}'.format(self.config.source_es_host,
                                               self.config.source_es_port))\
            .option('es.net.http.auth.user', self.config.source_es_user)\
            .option('es.net.http.auth.pass', self.config.source_es_pass)\
            .option('es.nodes.wan.only', 'true')\
            .option('es.nodes.resolve.hostname', 'false')\
            .option('es.read.field.exclude', self.config.case_exclude_fields)\
            .option('es.resource.read', source)\
            .load(source)

        # Add columns from maf_df
        maf_columns = ['available_variation_data']
        maf_data = (maf_df.select('case_id', *maf_columns)
                          .dropDuplicates(subset=['case_id'] + maf_columns))

        df = df.join(maf_data, on=['case_id'], how='left')

        self.logger.info('Repartitioning case dataframe')
        df = df.repartition(self.config.repartition, 'case_id')

        if self.config.cache_dataframes['cases']:
            self.logger.info('Caching repartitioned case dataframe')
            df.cache().count()
        return df
