import abc
import json
import logging

from normalizer.mapper import ModelMapper
from pyspark.sql.functions import col, size
from pyspark.sql.types import StructType

from config import LOG_FORMAT
from exports.builders.utils import percentile


logging.basicConfig(format=LOG_FORMAT)


def get_all_boolean_paths(mapping):
    res = []

    def helper(node, path=None):
        if path is None:
            path = []

        for key, value in node['properties'].items():
            if value.get('type') == 'boolean':
                res.append(path + [key])
            elif 'properties' in value:
                helper(value, path + [key])

    helper(mapping)
    return res


class BaseBuilder(object):
    """
    BaseBuilder contains the structure necessary for a Builder object.
    """
    __metaclass__ = abc.ABCMeta

    index_name = None
    id_field = None
    settings = None

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self.debug = config.debug

    @abc.abstractmethod
    def build(self, *args, **kwargs):
        """
        Contains the ETL logic to construct a spark dataframe of
        the same structure as the required output index.
        """
        pass

    def check_and_cast_booleans(self, df, mapping):
        paths = get_all_boolean_paths(mapping)
        schema_json = df.schema.jsonValue()
        modified = any(self.cast_path(path, schema_json) for path in paths)
        if modified:
            schema = StructType.fromJson(schema_json)
            select_expr = [df[f.name].cast(f.dataType) for f in schema.fields]
            df = df.select(*select_expr)

        return df

    def cast_path(self, path, cur):
        field = {}
        for node in path:
            for field in cur['fields']:
                if field['name'] == node:
                    cur = field['type']
                    if 'elementType' in cur:
                        cur = cur['elementType']
                    break
            else:
                self.log("{} not found in {}".format(path, self.index_name))
                break
        else:
            if cur != 'boolean' and 'type' in field:
                self.log("cast {} to boolean in {}".format(path, self.index_name))
                field['type'] = 'boolean'
                return True
        return False

    def load(self):
        """
        Responsible for loading the dataframe resulting from :func:`build`
        into a destination, usually Elasticsearch.
        """
        index = self.config.indices[self.index_name]

        index_mapper = ModelMapper(self.index_name)
        if self.config.skip_normalization:
            index_body = index_mapper.index_settings
        else:
            index_body = index_mapper.get_normalized_mappings()
        index_body = json.dumps(index_body)

        self.log('Creating {} index'.format(index))
        response = self.config.es.indices.create(index=index, body=index_body)
        self.log(response)

        self.log('Repartitioning {}'.format(self.index_name))
        df = getattr(self,
                     self.index_name).repartition(self.config.df_repartition,
                                                  self.id_field)

        df = self.check_and_cast_booleans(df, index_mapper.mapping)
        self.log('Exporting {} index to {}'.format(self.index_name, index))
        df.coalesce(self.config.df_coalesce).write\
            .format('org.elasticsearch.spark.sql')\
            .option('es.nodes', self.config.es_nodes)\
            .option('es.net.http.auth.user', self.config.source_es_user)\
            .option('es.net.http.auth.pass', self.config.es_pass)\
            .option('es.net.ssl', self.config.es_use_ssl)\
            .option('es.net.ssl.cert.allow.self.signed', self.config.disable_es_verify_certs)\
            .option('es.nodes.wan.only', 'true')\
            .option('es.nodes.resolve.hostname', 'false')\
            .option('es.resource.write', index)\
            .option('es.http.timeout', '20m')\
            .option('es.http.retries', '-1')\
            .option('es.batch.write.retry.count', '-1')\
            .option('es.batch.write.retry.wait', '10m')\
            .option('es.batch.size.bytes', self.config.batch_size_bytes)\
            .option('es.batch.size.entries', self.config.batch_size_entries)\
            .option('es.batch.write.refresh', False)\
            .option('es.mapping.id', self.id_field)\
            .save(index)
        self.log("Finished exporting {} index to {}".format(self.index_name, index))

        df.unpersist()

    def truncate_df_at_percentile(self,
                                  df_to_truncate,
                                  field,
                                  percentile_threshold,
                                  df_for_percentile_calculation=None):
        """
        Truncates df_to_truncate to remove rows
        where field > percentile_threshold
        """

        if percentile_threshold < 100:
            self.log('Calculating number of {}'.format(field))

            count_col_name = '{}_count'.format(field.replace('.', '_'))
            df_to_truncate = df_to_truncate.withColumn(count_col_name,
                                                       size(col(field)))
            if df_for_percentile_calculation is None:
                df_for_percentile_calculation = df_to_truncate
            else:
                df_for_percentile_calculation = \
                    df_for_percentile_calculation.withColumn(count_col_name,
                                                             size(col(field)))

            self.log('Calculating {} percentile'.format(percentile_threshold))
            threshold = percentile([
                                    int(r[count_col_name])
                                    for r in df_for_percentile_calculation.select(
                                        count_col_name).collect()
                                    ], percentile_threshold)

            self.log('Truncating dataframe'
                     '(removing rows where number of '
                     '{} > {})'.format(field, threshold))

            df_to_truncate = df_to_truncate.filter(
                '{} <= {}'.format(count_col_name, threshold)
                ).drop(count_col_name)

        return df_to_truncate

    def load_raw(self, path=None):
        """
        Loads the computed index's dataframe, if it exists, and return it,
        returns None it does not
        """
        if path is None:
            path = self.config.get_raw_output_path(self.index_name)
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
        if not self.config.output_raw == 'write':
            self.logger.info('Will not write raw output to s3')
            return

        if path is None:
            path = self.config.get_raw_output_path(self.index_name)

        df = getattr(self, self.index_name, None)
        assert df is not None, 'Builder does not have index_name attribute'

        # Repartition by the id into number of partitions specified in config
        id_field = getattr(self, self.id_field, None)
        if id_field:
            df = df.repartition(self.config.df_repartition, id_field).write
        else:
            df = df.repartition(self.config.df_repartition).write
            df = df.mode('overwrite')
        self.logger.info('Saving {} to {}'.format(self.index_name, path))
        df.json(path)

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
