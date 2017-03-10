from exports.builders.utils import percentile
from pyspark.sql.functions import col, size
import json
import requests
import logging

logging.basicConfig()


class BaseBuilder(object):
    """
    BaseBuilder contains the structure necessary for a Builder object.
    """

    index_name = None
    id_field = None
    mapper = None

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug

    def build(self):
        """
        Contains the ETL logic to construct a spark dataframe of
        the same structure as the required output index.
        """
        raise NotImplementedError

    def load(self):
        """
        Responsible for loading the dataframe resulting from :func:`build`
        into a destination, usually Elasticsearch.
        """
        index = self.config.indices[self.index_name]
        doc = self.config.index_names[self.index_name]
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(self.mapper(doc).settings)

        response = requests.put('{}:{}/{}'.format(self.config.es_host,
                                                  self.config.es_port,
                                                  index),
                                auth=(self.config.es_user, self.config.es_pass),
                                data=data)
        try:
            self.log(response.json())
        except ValueError as err:
            self.log(repr(err))

        self.log('Exporting {} index to {}'.format(self.index_name, index))
        getattr(self, self.index_name).coalesce(20).write\
            .format('org.elasticsearch.spark.sql')\
            .option('es.nodes', '{}:{}'.format(self.config.es_host,
                                               self.config.es_port))\
            .option('es.net.http.auth.user', self.config.es_user)\
            .option('es.net.http.auth.pass', self.config.es_pass)\
            .option('es.nodes.wan.only','true')\
            .option('es.nodes.resolve.hostname','false')\
            .option('es.resource.write', index_doc)\
            .option('es.http.timeout', '20m')\
            .option('es.http.retries', '-1')\
            .option('es.batch.write.retry.count', '-1')\
            .option('es.batch.write.retry.wait', '10m')\
            .option('es.batch.size.bytes','5mb')\
            .option('es.batch.size.entries', '100')\
            .option('es.mapping.id', self.id_field)\
            .option('es.spark.dataframe.write.null', 'true')\
            .save(index_doc)

    def truncate_df_at_percentile(self, df_to_truncate, field, percentile_threshold, df_for_percentile_calculation=None):
        """
        Truncates df_to_truncate to remove rows where field > percentile_threshold
        """

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

    def log(self, string):
        """
        Handles Builder logging.
        """
        self.logger.info(string)

    def log_count(self, dataframe):
        """
        Logs dataframe count if in Debug mode
        """
        if self.debug:
            self.log('Count: {}'.format(dataframe.count()))
