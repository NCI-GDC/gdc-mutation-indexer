from pyspark import sql
from pyspark.sql.functions import struct, collect_list, udf, col
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SQLContext

from config import BaseConfig

from exports import builders
from exports.builders.df_builders import (
    get_gene_df,
    build_ssm_subtree,
    build_cnv_subtree,
)


class CaseCentricBuilder(builders.BaseBuilder):
    """
    Builds case-centric dataframe given case and maf dataframes::

        case{}
             |___ gene[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
    """

    index_name = 'case_centric'
    id_field = 'case_id'

    def __init__(
        self, 
        config: BaseConfig, 
        sqlContext: SQLContext, 
        consequence_builder: builders.ConsequenceBuilder, 
        observation_builder: builders.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(self, maf_df: sql.DataFrame, gistic_df: sql.DataFrame, case_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
        """
        Builds Case Centric index
        """
        self.log('Building CaseCentric')
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == 'load':
            self.case_centric = self.load_raw()
            if self.case_centric is not None:
                return self

        self.log('Building Gene subtree')
        gene_subtree = self.build_gene_subtree(maf_df, gistic_df, primary_aliquot_df)

        self.log('Join Case with Gene subtree [left, case_id]')
        case_centric = (
            case_df.join(gene_subtree,
                         on=['case_id'], how='left')
        )
        self.log_count(case_centric)

        self.log('Finalizing case_centric build')
        case_centric = self._final_transform(case_centric)
        self.log_count(case_centric)

        self.case_centric = case_centric
        self.log('Build finished')

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_gene_subtree(self, maf_df: sql.DataFrame, gistic_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """

        # TODO Refactor with gene centric.
        self.log('Building Gene from MAF and Gistic')
        gene_df = get_gene_df(maf_df, self.index_name,
                              add_fields=['case_id'],
                              drop_fields=['canonical_transcript_length',
                                           'canonical_transcript_length_cds',
                                           'canonical_transcript_length_genomic'])

        gistic_gene_df = get_gene_df(gistic_df, self.index_name,
                                     add_fields=['case_id'],
                                     drop_fields=['canonical_transcript_length',
                                                  'canonical_transcript_length_cds',
                                                  'canonical_transcript_length_genomic'])

        gene_df = gene_df.union(gistic_gene_df).distinct()
        self.log_count(gene_df)

        self.log('Building SSM subtree')
        ssm_df = self.build_ssm_subtree(maf_df, primary_aliquot_df)
        self.log_count(ssm_df)

        self.log('Building CNV subtree')
        cnv_df = self.build_cnv_subtree(gistic_df)
        self.log_count(cnv_df)

        self.log('Join SSM and CNV subtrees to Gene [left, gene_id, case_id]')
        gene_ssm_cnv_df = (
            gene_df.join(ssm_df, on=['gene_id', 'case_id'], how='left')
                   .join(cnv_df, on=['gene_id', 'case_id'], how='left')
        )
        self.log_count(gene_ssm_cnv_df)

        self.log('Grouping SSM and CNV subtrees under Gene')
        gene_ssm_cnv_df = (
            gene_ssm_cnv_df.select('case_id',
                                   struct('ssm', 'cnv',
                                          *gene_df.drop('case_id').columns)
                                   .alias('gene'))
        )
        self.log_count(gene_df)

        self.log('Grouping by case_id and aggregating to list under "gene"')
        gene_ssm_cnv_df = (
            gene_ssm_cnv_df.groupBy(gene_ssm_cnv_df.case_id)
                           .agg(collect_list('gene').alias('gene'))
        )
        return gene_ssm_cnv_df

    def build_ssm_subtree(self, maf_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame) -> sql.DataFrame:
        """
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """
        # Consequence
        cons_df = self.consequence_builder.build_for_ssm(maf_df, self.index_name, join_gene=False)

        # Observation
        obs_df = self.observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self.index_name,
            selector='ssm',
        )
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, self.index_name,
                                   obs_df=obs_df)

        # Aggregate SSM
        self.log('Aggregating ssm by case_id and gene_id')
        ssm_df = (
            ssm_df.select(
             'gene_id', 'case_id',
             struct(*ssm_df.drop('gene_id')
                           .drop('case_id').columns).alias('ssm'))
            .groupBy(['gene_id', 'case_id'])
            .agg(collect_list('ssm').alias('ssm')))

        return ssm_df

    def build_cnv_subtree(self, gistic_df):
        """
        cnv[]
           |___ observation[]

        """

        # Observation
        obs_df = self.observation_builder.build_for_cnv(
            gistic_df,
            self.index_name,
            selector='cnv',
        )

        # Build the final cnv dataframe
        cnv_df = build_cnv_subtree(gistic_df, self.index_name, obs_df=obs_df)

        # Aggregate CNV
        self.log('Aggregating cnv by case_id and gene_id')
        cnv_df = (
            cnv_df.select(
             'gene_id', 'case_id',
             struct(*cnv_df.drop('gene_id')
                           .drop('case_id').columns).alias('cnv'))
            .groupBy(['gene_id', 'case_id'])
            .agg(collect_list('cnv').alias('cnv')))

        return cnv_df

    def _final_transform(self, case_centric):
        """
        Final case_centric dataframe transformation:

        - add 'available_variation_data'
        - truncate outliers
        """
        # Coerce any cases that didn't have variation data from None to []
        case_centric = case_centric.withColumn(
            'available_variation_data',
            udf(lambda x: [] if (x is None) else x,
                ArrayType(StringType()))(col('available_variation_data'))
        )

        # Truncate outliers
        threshold = self.config.percentile_threshold['genes_per_case']
        case_centric = self.truncate_df_at_percentile(case_centric, 'gene',
                                                      threshold)

        return case_centric
