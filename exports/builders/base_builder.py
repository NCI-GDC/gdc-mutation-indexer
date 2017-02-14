import logging

logging.basicConfig()


class BaseBuilder(object):

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug

    def log(self, string):
        self.logger.info(string)

    def log_count(self, dataframe):
        if self.debug:
            self.log('Count: {}'.format(dataframe.count()))
