import requests
import json

from pyspark.sql.functions import struct, collect_list

from exports.builders.df_builders import (
    get_gene_df,
    get_ssm_df
)
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

    def build_ssm(self, maf_df):
        self.log('Building SSM from MAF')
        # SSM
        ssm_df = get_ssm_df(
            maf_df, add_fields=['gene_id'], unique_fields=['ssm_id'])

        self.log_count(ssm_df)

        self.log('Building Transctipt')
        # Transcript
        cons_df = TranscriptBuilder(
            self.config, self.sqlContext).build(maf_df)

        self.log_count(cons_df)

        self.log('Aggregating Obs from MAF')
        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)
        self.log_count(obs_df)

        self.log('Join SSM with Transcripts [left, ssm_id]')
        df = (ssm_df.join(cons_df, ssm_df.ssm_id == cons_df.ssm_id, 'left')
              .drop(cons_df.ssm_id))
        self.log_count(df)

        self.log('Join Result above with Obs [left, ssm_id]')
        df = (df.join(obs_df, df.ssm_id == obs_df.ssm_id, 'left')
              .drop(obs_df.ssm_id))
        self.log_count(df)
        return df

    def build_gene(self, maf_df):
        self.log('Building Gene from MAF')
        # Build the gene from the maf
        gene_df = get_gene_df(maf_df, add_fields=['_case_submitter_id'])

        self.log_count(gene_df)

        ssm_df = self.build_ssm(maf_df)

        self.log('Aggregating ssm by _case_submitter_id and gene_id')
        ssm_df = (
            ssm_df.select(
             'gene_id', '_case_submitter_id',
             struct(*ssm_df.drop('gene_id')
                    .drop('_case_submitter_id')
                    .columns).alias('ssm'))
            .groupBy(['gene_id', '_case_submitter_id'])
            .agg(collect_list('ssm').alias('ssm')))
        self.log_count(ssm_df)

        self.log('Join ssm with Gene [inner, gene_id, _case_submitter_id]')

        join_condition = (
            (gene_df.gene_id == ssm_df.gene_id) &
            (gene_df._case_submitter_id == ssm_df._case_submitter_id))

        gene_ssm = (
            gene_df.join(ssm_df, join_condition)
            .drop(ssm_df.gene_id)
            .drop(ssm_df._case_submitter_id)
            .select('_case_submitter_id',
                    struct('ssm', *gene_df.drop('_case_submitter_id').columns)
                    .alias('gene')))
        self.log_count(gene_ssm)
        return gene_ssm

    def build(self, maf_df=None):
        '''
        '''
        self.logger.info('Building MAF')
        if maf_df is None:
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        self.log('Building Case')
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        gene_ssm = self.build_gene(maf_df)

        self.log('Final join Case with last join result [inner, submitter_id]')
        case_centric = (
            case_df.join(gene_ssm,
                         case_df.submitter_id == gene_ssm._case_submitter_id,
                         'inner')
            .drop(gene_ssm._case_submitter_id)
            .groupBy(*case_df.columns)
            .agg(collect_list('gene').alias('gene')))
        self.case_centric = case_centric
        self.log_count(case_centric)
        self.log('Build finished')
        return self

    def load(self):
        '''
        '''
        index = self.config.indices['case_centric']
        doc = self.config.index_names['case_centric']
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
