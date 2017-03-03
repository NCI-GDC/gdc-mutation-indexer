import json
import requests
import logging
logging.basicConfig()

from pyspark.sql.functions import lit, struct, collect_list

from exports.builders.df_builders import (
    get_gene_df,
    build_ssm_subtree,
)
from exports.builders import (
    MAFBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders import BaseBuilder
from exports.mappers import GeneMapper


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

    def build_ssm(self, maf_df):
        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build(maf_df)

        # Observation
        obs_df = ObservationBuilder(self.config, self.sqlContext).build(maf_df)

        # SSM
        ssm_df = build_ssm_subtree(maf_df, obs_df, cons_df)
        return ssm_df

    def build_case_with_gene_id(self, maf_df):
        self.log('\nSelecting Gene from MAF')
        gene_df = (get_gene_df(maf_df, add_fields=['_case_submitter_id'])
                        .select('_case_submitter_id', 'gene_id'))

        self.log("Building Case")
        case_df = CaseBuilder(self.config, self.sqlContext).build()
        self.log_count(case_df)

        self.log('Getting gene_id for each case via joining with gene_df')
        case_gene_id = (
                     gene_df.join(case_df,
                                  gene_df._case_submitter_id ==
                                  case_df.submitter_id,
                                  'inner')
                            .select('gene_id', *case_df.columns)
                    )
        return case_gene_id

    def build_case_ssm(self, maf_df):
        case_gene_id = self.build_case_with_gene_id(maf_df)

        ssm_df = self.build_ssm(maf_df)
        self.log('Aggregating ssm by _case_submitter_id and gene_id')
        ssm_df = (
                    ssm_df.select('gene_id',
                                  '_case_submitter_id',
                                  struct(*ssm_df
                                         .drop('gene_id')
                                         .drop('_case_submitter_id')
                                         .columns)
                                  .alias('ssm')
                                  )
                          .groupBy(['gene_id', '_case_submitter_id'])
                          .agg(collect_list('ssm').alias('ssm'))
                 )
        self.log_count(ssm_df)

        self.log("Joining Case+gene_id with SSM [inner, gene_id, submitter_id]")

        join_condition = (
            (case_gene_id.submitter_id == ssm_df._case_submitter_id) &
            (case_gene_id.gene_id == ssm_df.gene_id)
        )
        case_ssm = (
                    case_gene_id.join(ssm_df, join_condition, 'inner')
                                .drop(ssm_df.gene_id)
                                .select('_case_submitter_id',
                                        struct('ssm',
                                               *case_gene_id.columns)
                                        .alias('case'))
                    )
        self.log_count(case_ssm)

        return case_ssm

    def build(self, maf_df=None):
        """
        """
        self.log('\nBuilding GeneCentric')
        if maf_df is None:
            self.log('\nBuilding MAF')
            maf_df = MAFBuilder(self.config, self.sqlContext).build()
        self.log_count(maf_df)

        self.log('\nSelecting Gene from MAF')
        gene_df = get_gene_df(maf_df, unique_fields=['gene_id'])

        case_ssm = self.build_case_ssm(maf_df)

        case_ssm_grouped = (
            case_ssm.groupBy(case_ssm.case.gene_id.alias('gene_id'))
            .agg(collect_list('case').alias('case'))
        )

        self.log('\nJoining Gene with Case [inner, "gene_id"]')
        gene_centric = (
            gene_df.join(case_ssm_grouped,
                         gene_df.gene_id == case_ssm_grouped.gene_id,
                         'inner')
                   .drop(case_ssm._case_submitter_id)
        )

        self.log_count(gene_centric)
        self.gene_centric = gene_centric
        self.log_count(self.gene_centric)

        self.log('Build finished')
        return self

    def load(self):
        """
        """
        index = self.config.indices['gene_centric']
        doc = self.config.index_names['gene_centric']
        index_doc = '{}/{}'.format(index, doc)

        data = json.dumps(GeneMapper(doc).settings)

        self.log(requests.put('{}:{}/{}'.format(self.config.es_host,
                                                self.config.es_port,
                                                index),
                              auth=(self.config.es_user, self.config.es_pass),
                              data=data).json())

        self.log('Exporting gene centric index to {}'.format(index))
        self.gene_centric.coalesce(20).write.format('org.elasticsearch.spark.sql')\
                         .option('es.nodes', '{}:{}'
                                 .format(self.config.es_host,
                                         self.config.es_port))\
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
                         .option('es.mapping.id', 'gene_id')\
                         .option('es.spark.dataframe.write.null', 'true')\
                         .save(index_doc)
