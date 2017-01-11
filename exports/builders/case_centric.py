import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import MAFBuilder, CaseBuilder


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
                                *struct_select('../mappings/gene.yml'))\
                                .drop_duplicates()
        # SSM
        ssm_df = maf_df.select('gene_id',
                               *struct_select('../mappings/ssm.yml'))\
                                .drop_duplicates()
        # Consequence
        stmt = (struct(
                    struct(
                        struct(*struct_select('../mappings/annotation.yml'))
                            .alias('annotation'),
                           *struct_select('../mappings/transcript.yml')
                    ).alias('transcript')
                ).alias('consequence'))

        cons_df = maf_df.select('ssm_id', stmt)\
                        .groupBy('ssm_id')\
                        .agg(collect_list('consequence').alias('consequence'))

        # Observation
        obs_df = maf_df.select('ssm_id',
                               struct(*struct_select('../mappings/observation.yml'))
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
        case_df = CaseBuilder(self.config, self.sqlContext).build()

        case_centric = case_df.join(gene_ssm,
                                    case_df.submitter_id == gene_ssm._case_submitter_id,
                                    'left')\
                                .drop(gene_ssm._case_submitter_id)\
                                .groupBy(*case_df.columns)\
                                .agg(collect_list('gene').alias('gene'))

        #case_centric.printSchema()
        #obs_df.printSchema()

        self.case_centric = case_centric

        return self

    def load(self):
        '''
        '''
        index = self.config.indices['case_centric']
        doc = self.config.index_names['case_centric']
        index_doc = '{}/{}'.format(index, doc)

        from exports.mappers import CaseMapper
        m = CaseMapper()

        print requests.delete('http://{}:{}/{}'.format(self.config.es_host,
                                                       self.config.es_port,
                                                       index)).json()
        
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

        print requests.put('http://{}:{}/{}'.format(self.config.es_host,
                                                    self.config.es_port,
                                                    index), data=data).json()

        self.logger.info('Exporting case centric index')
        self.case_centric.coalesce(1).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                            .option('es.nodes.resolve.hostname','false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '10m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count','-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes','500mb')\
                            .option('es.batch.size.entries', '1')\
                            .option('es.mapping.id','case_id')\
                            .save(index_doc)
