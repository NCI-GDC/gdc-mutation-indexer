from pyspark.sql.functions import col, collect_list, lit, struct

from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from exports.builders.df_builders import (
    build_ssm_subtree,
    get_gene_df,
)
from exports.builders.gene_model import GeneModelBuilder
from exports.builders.utils import skew_join, uuid5_col


class SSMScoreCentricBuilder(BaseBuilder):
    """
    Builds SSM-score-centric dataframe given case and maf dataframes:

        ssm_score_centric{}
              |____ case{}
              |____ gene{}
              |____ ssm[]
                     |___ consequence[]
                     |             |_____ transcript{}
                     |                          |_____ annotation{}
                     |___ observation[]
    """

    index_name = 'ssm_score_centric'
    id_field = 'ssm_score_centric_id'

    def build(self, maf_df, case_df):
        """
        Builds SSM Score Centric index
        """
        self.log('Building SSMScoreCentric')

        case_subtree = self.build_case_subtree(case_df)
        gene_subtree = self.build_gene_subtree()
        ssm_subtree = self.build_ssm_subtree(maf_df)

        self.log('Building SSMScoreCentric from subtrees')

        score_df = ssm_subtree.join(
            case_subtree,
            on=(ssm_subtree.case_id == case_subtree.case.case_id))

        score_df = skew_join(score_df, gene_subtree, 'gene_id', 'gene.gene_id')

        score_df = self.add_ssm_score_centric_id(score_df)

        # Now that we're done joining things together, we don't need the
        # top-level gene or case ID columns.
        score_df = score_df.drop('case_id', 'gene_id')

        self.log_count(score_df)
        self.ssm_score_centric = score_df
        self.log('Build finished')

        return self

    def build_case_subtree(self, case_df):
        """Create a subtree with each case in a "case" struct."""
        self.log('Building SSMScoreCentric case subtree')

        if self.config.structure == 'nested':
            subtree_columns = case_df.columns
        else:
            subtree_columns = 'case_id'

        case_subtree = case_df.select(struct(subtree_columns).alias('case'))

        self.log_count(case_subtree)
        return case_subtree

    def build_gene_subtree(self):
        """Create a subtree with gene data in a "gene" struct."""
        self.log('Building SSMScoreCentric gene subtree')

        # Rebuilding this gene thing seems like less work than extracting it
        # from the MAF dataframe, especially if we've cached it.
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
        self.log('Building SSMScoreCentric SSM subtree')

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

    def add_ssm_score_centric_id(self, df):
        """Add ssm_score_centric_id column to the dataframe."""
        df = df.withColumn(self.id_field, uuid5_col(lit(self.id_field),
                                                    col('case_id'),
                                                    col('gene_id')))

        return df
