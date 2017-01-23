import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list

from exports.builders.utils import struct_select
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder


class GeneCentricBuilder(object):
    '''
    Builds gene-centric dataframe given case and maf dataframes

    gene{}
         |___ case[]
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
        self.logger.info('Building gene_centric dataframe')
        if maf_df is None:
            maf_df = MAFBuilder(self.config, self.sqlContext).build()

        # Build the gene from the maf
        gene_df = maf_df.select('_case_submitter_id',
                                *struct_select('gene.yml', ignore=['transcripts']))
        # SSM
        ssm_df = maf_df.select('_case_submitter_id',
                               *struct_select('ssm.yml'))

        # Consequence
        # stmt = (struct(
        #             struct(
        #                 struct(*struct_select('annotation.yml'))
        #                     .alias('annotation'),
        #                    *struct_select('transcript.yml')
        #             ).alias('transcript')
        #         ).alias('consequence'))

        # cons_df = maf_df.select('ssm_id', stmt)\
        #                 .groupBy('ssm_id')\
        #                 .agg(collect_list('consequence').alias('consequence'))

        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)

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

        df = df.select('_case_submitter_id', struct('consequence', 'observation', *ssm_df.drop('gene_id').drop('_case_submitter_id').columns).alias('ssm'))\
                    .groupBy('_case_submitter_id')\
                    .agg(collect_list('ssm').alias('ssm'))

        # Get cases from ES
        case_df = CaseBuilder(self.config, self.sqlContext).build()

        # Combine case with ssm tree
        case_ssm = case_df.join(df, case_df.submitter_id == df._case_submitter_id, 'left')\
                    .select('submitter_id', struct('ssm', *case_df.columns).alias('case'))

        gene_centric = gene_df.join(case_ssm,
                                    gene_df._case_submitter_id == case_ssm.submitter_id,
                                    'inner')\
                                .groupBy(*gene_df.columns)\
                                .agg(collect_list('case').alias('case'))\
                                .drop('_case_submitter_id')

        self.gene_centric= gene_centric

        return self

    def load(self, did=None):
        '''
        '''
        index = self.config.indices['gene_centric']
        doc = self.config.index_names['gene_centric'].replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        from exports.mappers import GeneMapper
        m = GeneMapper()

        data = json.dumps({"settings":{
                    "index":{
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
                                                    index), data=data).json()

        to_load = self.gene_centric
        if did:
            to_load = to_load.where(to_load.gene_id == did)

        self.logger.info('Exporting gene centric index')
        to_load.coalesce(20).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                            .option('es.nodes.resolve.hostname','false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '10m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count','-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes','500mb')\
                            .option('es.batch.size.entries', '1')\
                            .option('es.mapping.id','gene_id')\
                            .save(index_doc)
