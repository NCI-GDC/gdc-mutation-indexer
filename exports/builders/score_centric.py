from pyspark.sql.functions import col, collect_list, lit, struct

from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders.df_builders import (
    build_cnv_subtree,
    build_ssm_subtree,
    get_gene_df,
)
from exports.builders.gene_model import GeneModelBuilder
from exports.builders.utils import skew_join, uuid5_col


class ScoreCentricBuilder(BaseBuilder):
    """
    Builds score-centric dataframe given case, maf, and gistic dataframes:

        case{}
         |
        gene{}
         |
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

    index_name = 'score_centric'
    id_field = 'score_centric_id'

    def build(self, maf_df, gistic_df, case_df):
        """
        Builds Score Centric index
        """
        self.log('Building ScoreCentric')

        case_subtree = self.build_case_subtree(case_df)
        gene_subtree = self.build_gene_subtree()
        ssm_subtree = self.build_ssm_subtree(maf_df)
        cnv_subtree = self.build_cnv_subtree(gistic_df)

        self.log('Building ScoreCentric from subtrees')
        score_df = ssm_subtree.join(
            cnv_subtree,
            on=['case_id', 'gene_id'],
            how='outer')

        score_df = score_df.join(
            case_subtree,
            on=(score_df.case_id == case_subtree.case.case_id))

        score_df = skew_join(score_df, gene_subtree, 'gene_id', 'gene.gene_id')

        score_df = self.add_score_centric_id(score_df)

        # Now that we're done joining things together, we don't need the
        # top-level gene or case ID columns.
        score_df = score_df.drop('case_id', 'gene_id')

        self.log_count(score_df)
        self.score_centric = score_df
        self.log('Build finished')

        return self

    def build_case_subtree(self, case_df):
        """Create a subtree with each case in a "case" struct."""
        self.log('Building ScoreCentric case subtree')

        if self.config.structure == 'nested':
            subtree_columns = case_df.columns
        else:
            subtree_columns = 'case_id'

        case_subtree = case_df.select(struct(subtree_columns).alias('case'))

        self.log_count(case_subtree)
        return case_subtree

    def build_gene_subtree(self):
        """Create a subtree with gene data in a "gene" struct."""
        self.log('Building ScoreCentric gene subtree')

        # Rebuilding this gene thing seems like less work than extracting it
        # from the MAF/GISTIC dataframes, especially if we've cached it.
        gm_df = GeneModelBuilder(self.config, self.sqlContext).build()
        gm_df = gm_df.withColumnRenamed('_gene_id', 'gene_id')

        if self.config.structure == 'nested':
            gene_subtree = (
                get_gene_df(gm_df, self.index_name)
                .select(struct(col('*')).alias('gene'))
            )
        else:
            gene_subtree = gm_df.select(struct('gene_id').alias('gene'))

        self.log_count(gene_subtree)
        return gene_subtree

    def build_ssm_subtree(self, maf_df):
        """
        TODO: This branch is same as in case_centric and can be reused
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """
        self.log('Building ScoreCentric SSM subtree')

        # Consequence
        cons_df = ConsequenceBuilder(
            self.config, self.sqlContext).build_for_ssm(maf_df,
                                                        self.index_name)

        # Observation
        obs_df = ObservationBuilder().build_for_ssm(maf_df, self.index_name)
        obs_df = obs_df.drop('occurrence_id')

        # SSM
        ssm_df = build_ssm_subtree(maf_df, cons_df, self.index_name,
                                   obs_df=obs_df)

        # Aggregating SSM
        self.log('Aggregating ssm by case_id and gene_id')
        ssm_df = (
                    ssm_df.select('gene_id',
                                  'case_id',
                                  struct(*ssm_df.drop('gene_id')
                                                .drop('case_id').columns)
                                  .alias('ssm')
                                  )
                          .groupBy(['gene_id', 'case_id'])
                          .agg(collect_list('ssm').alias('ssm'))
                 )

        self.log_count(ssm_df)
        return ssm_df

    def build_cnv_subtree(self, gistic_df):
        """
        TODO: This branch is same as in case_centric and can be reused
        cnv[]
           |___ consequence[]
           |            |_____ gene{}
           |___ observation[]

        """
        self.log('Building ScoreCentric CNV subtree')

        # Consequence
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build_for_cnv(gistic_df, self.index_name))

        # Observation
        obs_df = ObservationBuilder().build_for_cnv(gistic_df, self.index_name)

        # Build the final cnv dataframe
        cnv_df = build_cnv_subtree(gistic_df, cons_df,
                                   self.index_name, obs_df=obs_df)

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

    def add_score_centric_id(self, df):
        """Add score_centric_id column to the dataframe."""
        df = df.withColumn(self.id_field, uuid5_col(lit(self.id_field),
                                                    col('case_id'),
                                                    col('gene_id')))

        return df
