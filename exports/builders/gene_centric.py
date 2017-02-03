import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, col, struct, collect_list
from exports.builders import MAFBuilder, CaseBuilder, TranscriptBuilder
from exports.builders.utils import struct_select


class GeneCentricBuilder(object):
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

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self, maf_df=None):
        """
        """
        self.logger.info('Building gene_centric dataframe')

        print '\nBuilding GeneCentric'

        if maf_df is None:
            print '\nBuilding MAF'
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        print 'maf_df count:', maf_df.count()

        print '\nBuilding Gene from MAF'
        # Build the gene from the maf
        gene_df = maf_df.select('_case_submitter_id',
                                *struct_select(self.config.mappings['gene'],
                                               ignore=['transcripts']))
        print 'gene_df count:', gene_df.count()

        print '\nBuilding SSM from MAF'
        # SSM
        ssm_df = maf_df.select('_case_submitter_id',
                               *struct_select(self.config.mappings['ssm']))
        print 'ssm_df count:', ssm_df.count()

        print '\nBuilding Transcript from MAF'
        transc_df = TranscriptBuilder(self.config, self.sqlContext).build(maf_df)
        print 'transc_df count:', transc_df.count()

        print '\nAggregating obs_df from MAF'
        # Observation
        obs_df = maf_df.select('ssm_id',
                               struct(*struct_select(self.config.mappings[
                                                         'observation']))
                               .alias('observation'))\
                       .groupBy('ssm_id')\
                       .agg(collect_list('observation').alias('observation'))
        print 'obs_df count:', obs_df.count()

        print '\nJoining ssm_df with transc_df [left, "ssm_id"]'
        df = ssm_df.join(transc_df, ssm_df.ssm_id == transc_df.ssm_id, 'left')\
                   .drop(transc_df.ssm_id)
        print 'df count:', df.count()

        print '\nJoining df with obs_df [left, "ssm_id"]'
        df = df.join(obs_df, df.ssm_id == obs_df.ssm_id, 'left')\
               .drop(obs_df.ssm_id)
        print 'df count:', df.count()

        print '\nAggregating df'
        df = df.select('_case_submitter_id',
                       struct('consequence', 'observation',
                              *ssm_df.drop('gene_id').drop('_case_submitter_id').columns)
                       .alias('ssm'))\
                       .groupBy('_case_submitter_id')\
                       .agg(collect_list('ssm').alias('ssm'))
        print 'df count:', df.count()

        print '\nBuilding case_df'
        # Get cases from ES
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        print 'case_df count:', case_df.count()


        print '\nJoining case_df with df [left, "submitter_id"]'
        # Combine case with ssm tree
        case_ssm = case_df.join(df, case_df.submitter_id == df._case_submitter_id,
                                'left')\
                          .select('submitter_id', struct('ssm', *case_df.columns)
                          .alias('case'))
        print 'case_ssm count:', case_ssm.count()

        print '\nFinal join (gene_df and case_ssm, [inner, "submitter_id"]) ' \
              'and aggregation'
        gene_centric = gene_df.join(case_ssm,
                                    gene_df._case_submitter_id == case_ssm.submitter_id,
                                    'inner')\
                              .groupBy(*gene_df.columns)\
                              .agg(collect_list('case').alias('case'))\
                              .drop('_case_submitter_id')
        print 'Final count:', gene_centric.count()

        self.gene_centric = gene_centric

        return self

    def load(self, did=None):
        """
        """
        index = self.config.indices['gene_centric']
        doc = self.config.index_names['gene_centric']  # .replace('_', '-')
        index_doc = '{}/{}'.format(index, doc)

        from exports.mappers import GeneMapper
        m = GeneMapper()

        data = json.dumps({"settings": {
                    "index": {
                        "refresh_interval": "1m",
                        "number_of_shards": 10,
                        "number_of_replicas": 0,
                        "mapper.dynamic": False,
                        "mapping.nested_fields.limit": 100,
                        "mapping.total_fields.limit": 2000
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
                    "mappings": {
                        doc: m.mapping
                    }})

        query = '{}:{}/{}'.format(self.config.es_host, self.config.es_port,
                                  index)
        requests.put(query, data=data).json()

        to_load = self.gene_centric

        if did:
            to_load = to_load.where(to_load.gene_id == did)

        self.logger.info('Exporting gene centric index')
        to_load.coalesce(20).write.format('org.elasticsearch.spark.sql')\
                            .option('es.nodes', '{}:{}'.format(self.config.es_host,
                                                               self.config.es_port))\
                            .option('es.nodes.resolve.hostname', 'false')\
                            .option('es.resource.write', index_doc)\
                            .option('es.http.timeout', '10m')\
                            .option('es.http.retries', '-1')\
                            .option('es.batch.write.retry.count', '-1')\
                            .option('es.batch.write.retry.wait', '10m')\
                            .option('es.batch.size.bytes', '500mb')\
                            .option('es.batch.size.entries', '1')\
                            .option('es.mapping.id', 'gene_id')\
                            .save(index_doc)
