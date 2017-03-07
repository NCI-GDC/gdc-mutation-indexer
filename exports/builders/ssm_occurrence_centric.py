import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list, udf

from exports.builders.utils import struct_select, uuid5_col
from exports.builders.df_builders import build_ssm_subtree
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.mappers import SSMOccurrenceMapper


class SSMOccurrenceCentricBuilder(BaseBuilder):
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

    index_name = 'ssm_occurrence_centric'

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build(maf_df, join_gene=True)

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df).drop('gene_id')
        self.log_count(ssm_df)

        ssm_cons = ssm_df.select('ssm_id',
                                 struct('consequence',
                                        *ssm_df.drop('_case_submitter_id').columns)\
                                 .alias('ssm'))
        self.log_count(ssm_cons)

        return ssm_cons

    
    def build_case(self, maf_df):
        self.log('Building case dataframe')
        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Join observation with case')
        case_obs_df = case_df.join(obs_df, case_df.submitter_id == obs_df._case_submitter_id, 'right')\
                        .select('case_id', 'ssm_id',
                            struct(
                                'observation',
                                *case_df.columns
                            ).alias('case'))\
                        .drop('_case_submitter_id')
        self.log_count(case_obs_df)
        return case_obs_df

    def build(self, maf_df=None):
        '''
        '''
        self.log('Building MAF')
        if maf_df is None:
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)


        case_obs_df = self.build_case(maf_df)

        ssm_cons = self.build_ssm(maf_df)


        self.log('Joining ssm with case')
        ssm_occurrence_centric = ssm_cons.join(case_obs_df, on='ssm_id', how='right')\
                                            .withColumn('ssm_occurrence_id',
                                                        uuid5_col(lit('ssm_occurrence'),
                                                            col('ssm_id'),
                                                            col('case_id')))\
                                            .drop('ssm_id')\
                                            .drop('case_id')\
                                            .drop('_case_submitter_id')
        self.log_count(ssm_occurrence_centric)

        # Generate ids
        self.ssm_occurrence_centric = ssm_occurrence_centric
        self.log('Build finished')
        return self

    def load(self, did=None):
        '''
        '''
        index = self.config.indices['ssm_occurrence_centric']
        doc = self.config.index_names['ssm_occurrence_centric']
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(SSMOccurrenceMapper(doc).settings)

        self.log(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index),
                              auth=(self.config.es_user, self.config.es_pass),
                              data=data).json())

        to_load = self.ssm_occurrence_centric
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
                            .option('es.mapping.id','ssm_occurrence_id')\
                            .option('es.spark.dataframe.write.null', 'true')\
                            .save(index_doc)
