from exports.builders.utils import percentile
from pyspark.sql.functions import col, size
from elasticsearch import Elasticsearch
import json
import logging
import os

from ..mappers.models_mapper import ModelMapper

logging.basicConfig()


class BaseBuilder(object):
    """
    BaseBuilder contains the structure necessary for a Builder object.
    """
    index_name = None
    id_field = None
    settings = None

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug
        self.es = Elasticsearch(self.config.es_host,
                                port=self.config.es_port,
                                http_auth=(self.config.es_user,
                                           self.config.es_pass))

    
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

        index_body = ModelMapper(self.index_name).create_index_settings()
        index_body = json.dumps(index_body)

        self.log('Creating {} index'.format(index))
        response = self.es.indices.create(index=index, ignore=400, body=index_body)
        self.log(response)

        self.save_build_metadata()

        self.log('Repartitioning {}'.format(self.index_name))
        df = getattr(self, self.index_name).repartition(self.config.repartition, self.id_field)

        if self.config.cache_dataframes[self.index_name]:
            self.log('Caching repartitioned {} dataframe'.format(self.index_name))
            df.cache().count()

        self.log('Exporting {} index to {}'.format(self.index_name, index))
        df.coalesce(self.config.coalesce).write\
            .format('org.elasticsearch.spark.sql')\
            .option('es.nodes', self.config.es_nodes)\
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
            .option('es.mapping.id', self.id_field)\
            .option('es.spark.dataframe.write.null', 'true')\
            .save(index_doc)

        df.unpersist()

    def truncate_df_at_percentile(self, df_to_truncate, field,
                                  percentile_threshold, df_for_percentile_calculation=None):
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

    def get_existing(self, path=None):
        """
        Loads the computed index's dataframe, if it exists, and return it,
        returns None it does not
        """
        if path == None:
            path = self.config.index_paths[self.index_name]
        try:
            self.logger.info('Using existing index from {}'.format(path))
            df = self.sqlContext.read.load(path)
            return df
        except Exception:
            self.logger.info('Couldn\'t find file at {}'.format(path))
            return None

    def write(self, path=None):
        """
        Writes the built dataframe to a json file at path
        """
        if path == None:
            path = self.config.index_paths[self.index_name]

        df = getattr(self, self.index_name, None)
        assert df != None, 'Builder does not have index_name attribute'

        # Repartition by the id into number of partitions specified in config
        id_field = getattr(self, self.id_field, None)
        if id_field:
            df = df.repartition(self.config.repartition, id_field).write
        else:
            df = df.repartition(self.config.repartition).write
        if self.config.index_overwrite:
            df = df.mode('overwrite')
        self.logger.info('Saving {} to {}'.format(self.index_name, path))
        df.json(path)

    def save_build_metadata(self):
        """
        Saves metadata about the build in a 'build_metadata' document in the
        elasticsearch index
        """

        index = self.config.indices[self.index_name]
        nb_mutations = -1

        if hasattr(self.config, 'nb_mutations'):
            nb_mutations = self.config.nb_mutations

        if '_rev_' in __file__:
            # The egg name is gdc_mutation_indexer-0.1.0_rev_COMMITHASH-py2.7.egg
            commit_hash = __file__.split('_rev_')[1].split('-')[0]
        else:
            if os.system('git rev-parse 2> /dev/null > /dev/null') == 0:
                commit_hash = os.system('git rev-parse HEAD')
            else:
                self.logger.error("Can't get commit hash. Either git is not installed or we are not "
                                  "in a git repo. If running on a spark cluster, make sure "
                                  "the egg name is gdc_mutation_indexer-X.Y.Z_rev_COMMITHASH-py2.7.egg")
                commit_hash = 'not found'

        metadata_doc = {
                'commit_hash': commit_hash,
                'indices_built': [k for k,v in self.config.index_names.iteritems() if v],
                'number_of_mutations': nb_mutations,
                'number_of_projects': len(self.config.maf_urls),
                'debug': self.config.debug,
                'maf_urls': self.config.maf_urls,
                'percentile_threshold': [{'name': k, 'value': v}
                                         for k, v in (self.config
                                                          .percentile_threshold
                                                          .iteritems())],
                'coalesce': self.config.coalesce,
                'repartition': self.config.repartition,
                'batch_size_bytes': self.config.batch_size_bytes,
                'batch_size_entries': int(self.config.batch_size_entries)
                }

        self.log('Saving build metadata')

        response = self.es.create(index=index, doc_type='build_metadata',
                                  id=0, body=metadata_doc)
        self.log(response)

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
