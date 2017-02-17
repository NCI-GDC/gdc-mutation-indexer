import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    TranscriptBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.mappers import SSMMapper


class SSMCentricBuilder(BaseBuilder):
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

    index_name = 'ssm_centric'

    def build(self, maf_df=None):
        '''
        '''
        if maf_df is None:
            self.log('Building MAF...')
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        # SSM
        ssm_df = maf_df.select(*struct_select(self.config.mappings['ssm']))

        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df, join_gene=True)

        # Observation
        self.log('Aggregating Observation from MAF')
        obs_df = ObservationBuilder(self.config, self.sqlContext)\
                    .build(maf_df, by='_case_submitter_id')

        # Get ssm from ES
        self.log("Building ssm_centric")
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Joining Cases with Observation, [right, submitter_id]')
        occurrence_df = case_df.join(obs_df, case_df.submitter_id == obs_df._case_submitter_id, 'right')\
                        .select('ssm_id', struct(
                            struct(
                                'observation',
                                *case_df.columns
                            ).alias('case')
                        ).alias('occurrence'))\
                        .groupby('ssm_id')\
                        .agg(collect_list('occurrence').alias('occurrence'))
        self.log_count(occurrence_df)

        self.log('Final join SSM + Transcript + Last one')
        ssm_centric = ssm_df.join(cons_df, ssm_df.ssm_id == cons_df.ssm_id)\
                        .drop(cons_df.ssm_id)\
                        .join(occurrence_df, ssm_df.ssm_id == occurrence_df.ssm_id)\
                        .drop(cons_df.ssm_id)
        self.log_count(ssm_centric)

        self.ssm_centric = ssm_centric
        self.log('Build finished')
        return self

    def load(self, did=None):
        '''
        '''
        index = self.config.indices['ssm_centric']
        doc = self.config.index_names['ssm_centric'].replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(SSMMapper(doc).settings)

        self.log(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index),
                              auth=(self.config.es_user, self.config.es_pass),
                              data=data).json())

        to_load = self.ssm_centric
        if did:
            to_load = to_load.where(to_load.ssm_id == did)

        self.log('Exporting ssm centric index to {}'.format(index))
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
                            .option('es.spark.dataframe.write.null', 'true')\
                            .save(index_doc)
