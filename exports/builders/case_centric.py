import requests
import json

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    TranscriptBuilder,
    ObservationBuilder
)
from exports.builders import BaseBuilder
from exports.mappers import CaseMapper


class CaseCentricBuilder(BaseBuilder):
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

    def build(self, maf_df=None):
        '''
        '''
        self.logger.info('Building MAF')
        if maf_df is None:
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        self.log('Building Gene from MAF')
        # Build the gene from the maf
        gene_df = maf_df.select('_case_submitter_id',
                                *struct_select('gene.yml', ignore=['transcripts']))\
                                .drop_duplicates()
        self.log_count(gene_df)

        self.log('Building SSM from MAF')
        # SSM
        ssm_df = maf_df.select('gene_id',
                               *struct_select('ssm.yml'))\
                                .drop_duplicates(['ssm_id'])
        self.log_count(ssm_df)

        self.log('Building Transctipt')
        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)\
                               .drop_duplicates(['ssm_id'])
        self.log_count(cons_df)

        self.log('Aggregating Obs from MAF')
        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        self.log_count(obs_df)

        self.log('Join SSM with Transcripts [left, ssm_id]')
        df = ssm_df.join(cons_df, ssm_df.ssm_id == cons_df.ssm_id, 'left')\
                    .drop(cons_df.ssm_id)
        self.log_count(df)

        self.log('Join Result above with Obs [left, ssm_id]')
        df = df.join(obs_df, df.ssm_id == obs_df.ssm_id, 'left')\
                    .drop(obs_df.ssm_id)
        self.log_count(df)

        self.log('Aggregating Result above')
        df = df.select('gene_id', struct('consequence', 'observation', *ssm_df.drop('gene_id').columns).alias('ssm'))\
                    .groupBy('gene_id')\
                    .agg(collect_list('ssm').alias('ssm'))
        self.log_count(df)

        self.log('Join Result above with Gene [inner, gene_id]')
        gene_ssm = gene_df.join(df, gene_df.gene_id == df.gene_id)\
                            .drop(df.gene_id)\
                            .select('_case_submitter_id', struct('ssm',*gene_df.drop('_case_submitter_id').columns).alias('gene'))
        self.log_count(gene_ssm)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Final join Case with last join result [inner, submitter_id]')
        case_centric = case_df.join(gene_ssm,
                                    case_df.submitter_id == gene_ssm._case_submitter_id,
                                    'inner')\
                                .drop(gene_ssm._case_submitter_id)\
                                .groupBy(*case_df.columns)\
                                .agg(collect_list('gene').alias('gene'))
        self.case_centric = case_centric
        self.log_count(case_centric)
        self.log('Build finished')
        return self

    def load(self):
        '''
        '''
        index = self.config.indices['case_centric']
        doc = self.config.index_names['case_centric'].replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(CaseMapper(doc).settings)

        self.log(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index),
                              auth=(self.config.es_user, self.config.es_pass),
                              data=data).json())

        self.log('Exporting case centric index to {}'.format(index))
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
                            .option('es.spark.dataframe.write.null', 'true')\
                            .save(index_doc)
