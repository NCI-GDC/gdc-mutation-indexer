import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list, udf

from exports.builders.utils import struct_select, ssm_occurrence_uuid_udf
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder


class SSMOccurrenceCentricBuilder(object):
    '''
    Builds ssm-occurrence-centric dataframe given case and maf dataframes

    ssm_occurrence{}
          |____ ssm{}
          |        |____ consequence[]
          |                     |_____ transcript{}
          |                                   |_____ gene{}
          |                                   |_____ annotation{}
          |____ case{}
                   |____ observation[]
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self, maf_df=None):
        '''
        '''
        self.logger.info('Building case dataframe')
        if maf_df is None:
            maf_df = MAFBuilder(self.config, self.sqlContext).build()

        # SSM
        ssm_df = maf_df.select('_case_submitter_id', *struct_select('ssm.yml'))

        # Consequence
        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)

        # Observation
        obs_df = maf_df.select('_case_submitter_id', 'ssm_id',
                               struct(*struct_select('observation.yml'))
                                      .alias('observation'))\
                        .groupby('_case_submitter_id', 'ssm_id')\
                        .agg(collect_list('observation').alias('observation'))

        # Get cases from ES
        case_df = CaseBuilder(self.config, self.sqlContext).build()

        case_obs_df = case_df.join(obs_df, case_df.submitter_id == obs_df._case_submitter_id, 'right')\
                        .select('submitter_id', 'case_id',
                            struct(
                                'observation',
                                *case_df.columns
                            ).alias('case'))

        uuid_func = ssm_occurrence_uuid_udf(self.config.ssm_namespace)

        ssm_occurrence_centric = ssm_df.join(case_obs_df, ssm_df._case_submitter_id == case_obs_df.submitter_id)\
                                    .drop(ssm_df._case_submitter_id)\
                                    .drop(case_obs_df.submitter_id)\
                                    .join(cons_df, ssm_df.ssm_id == cons_df.ssm_id)\
                                    .drop(cons_df.ssm_id)\
                                    .withColumn('ssm_occurrence_id', uuid_func(
                                                                        col('ssm_id'),
                                                                        col('case_id')))\
                                    .select(
                                        struct('consequence',*ssm_df.drop('_case_submitter_id').columns).alias('ssm'),
                                        'case',
                                        'ssm_occurrence_id',
                                    )

        self.ssm_occurrence_centric = ssm_occurrence_centric

        return self

    def load(self, did=None):
        '''
        '''
        index = self.config.indices['ssm_occurrence_centric']
        doc = self.config.index_names['ssm_occurrence_centric']
        index_doc = '{}/{}'.format(index, doc)

        from exports.mappers import SSMOccurrenceMapper
        m = SSMOccurrenceMapper()
        
        data = json.dumps({"settings":{"index":{
                        "refresh_interval":"1m",
                        "number_of_shards":1,
                        "number_of_replicas":0,
                        "mapper.dynamic":False,
                        "mapping.nested_fields.limit":100,
                        "mapping.total_fields.limit":2000
                    }},"mappings":{
                        doc: m.mapping
                    }})

        print requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index), data=data).json()

        to_load = self.ssm_occurrence_centric
        if did:
            to_load = to_load.where(to_load.ssm_id == did)

        self.logger.info('Exporting ssm centric index')
        to_load.coalesce(1).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                            .option('es.nodes.resolve.hostname','false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '10m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count','-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes','500mb')\
                            .option('es.batch.size.entries', '1')\
                            .option('es.mapping.id','ssm_occurrence_id')\
                            .save(index_doc)
