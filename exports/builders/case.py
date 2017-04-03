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

    def build(self):
        '''
        '''
        df = self.load()
        return df

    def load(self):
        '''
        Loads case docs from the gdc_from_graph index into a dataframe
        '''
        source = '{}/{}'.format(self.config.graph_index, self.config.graph_document)

        df = self.sqlContext.read.format("es")\
            .option('es.nodes', self.config.source_es_host)\
            .option('es.net.http.auth.user', self.config.es_user)\
            .option('es.net.http.auth.pass', self.config.es_pass)\
            .option('es.nodes.wan.only','true')\
            .option('es.nodes.resolve.hostname','false')\
            .option('es.read.field.exclude', self.config.case_exclude_fields)\
            .option('es.resource.read', source)\
            .load(source)

        self.logger.info('Repartitioning case dataframe')
        df = df.repartition(self.config.repartition, 'case_id')

        if self.config.cache_dataframes['cases']:
            self.logger.info('Caching repartitioned case dataframe')
            df.cache().count()
        return df
