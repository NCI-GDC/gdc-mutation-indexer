from exports.builders.utils import percentile
from pyspark.sql.functions import col, size
from elasticsearch import Elasticsearch
import subprocess
import logging

logging.basicConfig()


class BaseBuilder(object):
    """
    BaseBuilder contains the structure necessary for a Builder object.
    Currently, that is only the :func:`build` and :func:`load` methods
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug
        self.es = Elasticsearch(self.config.es_host,
                             port=self.config.es_port,
                             http_auth=(self.config.es_user, self.config.es_pass))

    def log(self, string):
        """
        Handles Builder logging.
        """
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
        raise NotImplementedError


    def save_build_metadata(self, index):
        '''
        Saves metadata about the build in a 'build_metadata' document in the elasticsearch index
        '''

        metadata_doc = {
                'commit_hash': subprocess.check_output(["git", "rev-parse", "HEAD"]).strip(),
                'number_of_mutations': self.config.nb_mutations,
                'number_of_projects': len(self.config.maf_urls),
                'debug': self.config.debug,
                'maf_urls': self.config.maf_urls,
                'percentile_threshold': [ {'name': k, 'value': v} for k,v in self.config.percentile_threshold.iteritems() ],
                'coalesce': self.config.coalesce,
                'batch_size_bytes': self.config.batch_size_bytes,
                'batch_size_entries': int(self.config.batch_size_entries)
                }

        self.log('Saving build metadata')
        res = self.es.create(index=index, doc_type='build_metadata', id=0, body=metadata_doc)
        self.log(res)


    def load_to_elasticsearch(self, index, doc, settings, data, id_mapping):
        '''
        '''
        self.log('Creating {} index'.format(index))
        res = self.es.indices.create(index=index, ignore=400, body=settings)
        self.log(res)

        self.save_build_metadata(index)

        index_doc = '{}/{}'.format(index, doc)

        self.log('Exporting {} index to {}'.format(doc.replace('_', ' '), index))
        data.coalesce(self.config.coalesce).write.format('org.elasticsearch.spark.sql')\
                         .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                         .option('es.net.http.auth.user', self.config.es_user)\
                         .option('es.net.http.auth.pass', self.config.es_pass)\
                         .option('es.nodes.wan.only', 'true')\
                         .option('es.nodes.resolve.hostname', 'false')\
                         .option('es.resource.write', index_doc)\
                         .option('es.http.timeout', '20m')\
                         .option('es.http.retries', '-1')\
                         .option('es.batch.write.retry.count', '-1')\
                         .option('es.batch.write.retry.wait', '10m')\
                         .option('es.batch.size.bytes', self.config.batch_size_bytes)\
                         .option('es.batch.size.entries', self.config.batch_size_entries)\
                         .option('es.mapping.id', id_mapping)\
                         .option('es.spark.dataframe.write.null', 'true')\
                         .save(index_doc)

