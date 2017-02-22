from exports.builders.utils import percentile
from pyspark.sql.functions import col, size
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

    def truncate_df_at_percentile(self, df_to_truncate, field, percentile_threshold, df_for_percentile_calculation=None):
        '''
        Truncates df_to_truncate to remove rows where field > percentile_threshold
        '''

        if percentile_threshold < 100:
            self.log('Calculating number of {}'.format(field))
            count_col_name = '{}_count'.format(field.replace('.', '_'))
            df_to_truncate = df_to_truncate.withColumn(count_col_name, size(col(field)))
            if df_for_percentile_calculation is None:
                df_for_percentile_calculation = df_to_truncate
            else:
                df_for_percentile_calculation = df_for_percentile_calculation.withColumn(count_col_name, size(col(field)))

            self.log('Calculating {} percentile'.format(percentile_threshold))
            threshold = percentile([int(r[count_col_name]) for r in df_for_percentile_calculation.select(count_col_name).collect()], percentile_threshold)

            self.log('Truncating dataframe (removing rows where number of {} > {})'.format(field, threshold))
            df_to_truncate = df_to_truncate.filter('{} <= {}'.format(count_col_name, threshold)).drop(count_col_name)

        return df_to_truncate
