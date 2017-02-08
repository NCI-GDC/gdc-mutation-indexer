import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder
from exports.mappers import CaseMapper


class CaseCentricBuilder(object):
    '''
    Builds case-centric dataframe given case and maf dataframes

    case{}
         |___ gene[]
                 |___ ssm[]
                       |___ consequence[]
                       |             |_____ transcript{}
                       |                          |_____ annotation{}
                       |___ observation[]
    '''

    index_name = 'case_centric'

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

        # Build the gene from the maf
        gene_df = maf_df.select('_case_submitter_id',
                                *struct_select('gene.yml', ignore=['transcripts']))\
                                .drop_duplicates()
        # SSM
        ssm_df = maf_df.select('gene_id',
                               *struct_select('ssm.yml'))\
                                .drop_duplicates(['ssm_id'])

        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)\
                               .drop_duplicates(['ssm_id'])

        # Observation
        obs_df = maf_df.select('ssm_id',
                               struct(*struct_select('observation.yml'))
                               .alias('observation'))\
                               .groupBy('ssm_id')\
                               .agg(collect_list('observation').alias('observation'))

        df = ssm_df.join(cons_df, ssm_df.ssm_id == cons_df.ssm_id, 'left')\
                    .drop(cons_df.ssm_id)

        df = df.join(obs_df, df.ssm_id == obs_df.ssm_id, 'left')\
                    .drop(obs_df.ssm_id)

        df = df.select('gene_id', struct('consequence', 'observation', *ssm_df.drop('gene_id').columns).alias('ssm'))\
                    .groupBy('gene_id')\
                    .agg(collect_list('ssm').alias('ssm'))

        gene_ssm = gene_df.join(df, gene_df.gene_id == df.gene_id)\
                            .drop(df.gene_id)\
                            .select('_case_submitter_id', struct('ssm',*gene_df.drop('_case_submitter_id').columns).alias('gene'))

        # Get cases from ES
        self.logger.info("Building case_centric")
        case_df = CaseBuilder(self.config, self.sqlContext).build()

        case_centric = case_df.join(gene_ssm,
                                    case_df.submitter_id == gene_ssm._case_submitter_id,
                                    'inner')\
                                .drop(gene_ssm._case_submitter_id)\
                                .groupBy(*case_df.columns)\
                                .agg(collect_list('gene').alias('gene'))

        self.case_centric = case_centric

        return self

    def load(self):
        '''
        '''
        index = self.config.indices['case_centric']
        doc = self.config.index_names['case_centric']  # .replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(CaseMapper(doc).settings)

        self.logger.info(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                        self.config.es_port,
                                                        index),
                                      auth=(self.config.es_user, self.config.es_pass),
                                      data=data).json())

        self.logger.info('Exporting case centric index')

        self.case_centric.coalesce(20).write.format('org.elasticsearch.spark.sql')\
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
                            .option('es.mapping.id','case_id')\
                            .save(index_doc)
