import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder
from exports.mappers import GeneMapper
from exports.builders.utils import struct_select
from exports.builders.base_builder import BaseBuilder


class GeneCentricBuilder(BaseBuilder):
    """
    Builds gene-centric dataframe given case and maf dataframes

    gene{}
         |___ case[]
                 |___ ssm[]
                       |___ consequence[]
                       |             |_____ transcript{}
                       |                          |_____ annotation{}
                       |___ observation[]
    """

    index_name = 'gene_centric'

    def build(self, maf_df=None):
        """
        """
        self.log('Building gene_centric dataframe')

        self.log('\nBuilding GeneCentric')

        if maf_df is None:
            print '\nBuilding MAF'
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        self.log('\nBuilding Gene from MAF')
        # Build the gene from the maf
        gene_df = maf_df.select('_case_submitter_id',
                                *struct_select('gene.yml', ignore=['transcripts']))
        # SSM
        ssm_df = maf_df.select('_case_submitter_id',
                               *struct_select('ssm.yml'))\
                               .drop_duplicates(['ssm_id'])

        cons_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)\
                               .drop_duplicates(['ssm_id'])

        self.log('\nAggregating obs_df from MAF')
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

        # Get genes from ES
        self.log("Building gene_centric")
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        # Combine case with ssm tree
        case_ssm = case_df.join(df, case_df.submitter_id == df._case_submitter_id, 'left')\
                    .select('submitter_id', struct('ssm', *case_df.columns).alias('case'))

        self.log('\nJoining case_df with df [left, "submitter_id"]')
        # Combine case with ssm tree
        case_ssm = case_df.join(df, case_df.submitter_id == df._case_submitter_id,
                                'left')\
                          .select('submitter_id', struct('ssm', *case_df.columns)
                          .alias('case'))
        self.log_count(case_ssm)

        self.log('\nFinal join (gene_df and case_ssm, [inner, "submitter_id"]) ' \
                 'and aggregation')
        gene_centric = gene_df.join(case_ssm,
                                    gene_df._case_submitter_id == case_ssm.submitter_id,
                                    'inner')\
                              .groupBy(*gene_df.columns)\
                              .agg(collect_list('case').alias('case'))\
                              .drop('_case_submitter_id')
        self.log_count(gene_centric)

        self.gene_centric = gene_centric
        self.log('Build finished')
        return self

    def load(self, did=None):
        """
        """
        index = self.config.indices['gene_centric']
        doc = self.config.index_names['gene_centric'].replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(GeneMapper(doc).settings)

        self.log(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index),
                              auth=(self.config.es_user, self.config.es_pass),
                              data=data).json())

        to_load = self.gene_centric

        if did:
            to_load = to_load.where(to_load.gene_id == did)

        self.log('Exporting gene centric index')
        to_load.coalesce(20).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host, self.config.es_port))\
                            .option('es.net.http.auth.user', self.config.es_user)\
                            .option('es.net.http.auth.pass', self.config.es_pass)\
                            .option('es.nodes.wan.only','true')\
                            .option('es.nodes.resolve.hostname','false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '20m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count', '-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes','5mb')\
                            .option('es.batch.size.entries', '100')\
                            .option('es.mapping.id','gene_id')\
                            .save(index_doc)
