import os
import yaml
import requests
from requests.auth import HTTPBasicAuth
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder


class SSMCentricBuilder(object):
    '''
    Builds ssm-centric dataframe given case and maf dataframes

    ssm{}
      |____ consequence[]
      |           |_____ transcript{}
      |                        |_____ gene{}
      |                        |_____ annotation{}
      |____ occurrence[]
                  |_____ case{}
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
        ssm_df = maf_df.select(*struct_select('ssm.yml'))

        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df, join_gene=True)

        # Observation
        obs_df = maf_df.select('_case_submitter_id', 'ssm_id',
                               struct(*struct_select('observation.yml'))
                                      .alias('observation'))\
                        .groupby('_case_submitter_id', 'ssm_id')\
                        .agg(collect_list('observation').alias('observation'))

        # Get cases from ES
        case_df = CaseBuilder(self.config, self.sqlContext).build()

        occurrence_df = case_df.join(obs_df, case_df.submitter_id == obs_df._case_submitter_id, 'right')\
                        .select('ssm_id', struct(
                            struct(
                                'observation',
                                *case_df.columns
                            ).alias('case')
                        ).alias('occurrence'))\
                        .groupby('ssm_id')\
                        .agg(collect_list('occurrence').alias('occurrence'))

        ssm_centric = ssm_df.join(cons_df, ssm_df.ssm_id == cons_df.ssm_id)\
                        .drop(cons_df.ssm_id)\
                        .join(occurrence_df, ssm_df.ssm_id == occurrence_df.ssm_id)\
                        .drop(cons_df.ssm_id)

        self.ssm_centric = ssm_centric

        return self

    def load(self, did=None):
        '''
        '''
        index = self.config.indices['ssm_centric']
        doc = self.config.index_names['ssm_centric'].replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        from exports.mappers import SSMMapper
        m = SSMMapper()

        data = json.dumps({"settings":{"index":{
                        "refresh_interval":"1m",
                        "number_of_shards":10,
                        "number_of_replicas":0,
                        "mapper.dynamic":False,
                        "mapping.nested_fields.limit":100,
                        "mapping.total_fields.limit":2000
                    },
                    "analysis": {
                        "analyzer": {
                            "id_index": { 
                                "filter": ["lowercase", "edge_ngram"],
                                "type": "custom",
                                "tokenizer": "whitespace"
                            },
                            "id_search": {
                                "filter": ["lowercase"],
                                "type": "custom",
                                "tokenizer": "whitespace"
                            }
                        }
                    }},
                    "mappings":{
                        doc: m.mapping
                    }})

        print requests.put('{}:{}/{}'.format(self.config.es_host,
                                                    self.config.es_port,
                                                    index),
                           auth=HTTPBasicAuth(self.config.es_user, self.config.es_pass),
                           data=data).json()

        to_load = self.ssm_centric
        if did:
            to_load = to_load.where(to_load.ssm_id == did)

        self.logger.info('Exporting ssm centric index')
        to_load.coalesce(20).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                            .option('es.net.http.auth.user', self.config.es_user)\
                            .option('es.net.http.auth.pass', self.config.es_pass)\
                            .option('es.nodes.wan.only','true')\
                            .option('es.nodes.resolve.hostname','false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '20m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count','-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes','5mb')\
                            .option('es.batch.size.entries', '100')\
                            .option('es.mapping.id','ssm_id')\
                            .save(index_doc)
