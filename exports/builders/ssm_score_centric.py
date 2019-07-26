from pyspark.sql.functions import col, collect_list, lit, struct, udf
from pyspark.sql.types import StringType

from . import (
    BaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)
from .df_builders import (
    build_ssm_subtree,
    get_gene_df,
)
from .gene_model import GeneModelBuilder
from .utils import skew_join, uuid5_col

from ..mappers.model_mapper import ModelMapper


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

    def __init__(self, config, sqlContext):
        super(SSMScoreCentricBuilder, self).__init__(config, sqlContext)
        self.routing_column = '_routing_'

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
        score_df = self.add_routing_column(score_df)

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

    def add_routing_column(self, df):
        """Add the column for routing documents to shards during indexing."""

        # TODO Try again with gene_split 2 or 3.

        # TODO If 2 or 3 does better than 1, make gene_split configurable.
        # If gene_split 1 is way better, maybe we should just hash the case ID.

        # Hash the case and gene IDs to arrange documents as follows:
        #
        # 1. Route all documents for a given case/gene pair to the same shard.
        # 2. Route all documents for a given case to the same N shards, and
        #    divide cases evenly among the respective N-shard groupings.
        # 3. Split genes evenly within each group of N shards (the value of N
        #    is therefore referred to as the "gene split").
        #
        # Limit the maximum number of cases or genes per shard in this way
        # and hopefully make terms aggregations go faster.
        gene_split = 1

        index_settings = ModelMapper(self.index_name).index_settings
        num_shards = index_settings['settings']['index']['number_of_shards']

        # Make sure we really can divide evenly with these settings.
        case_split, remainder = divmod(num_shards, gene_split)
        assert remainder == 0

        def compute_routing(case_id, gene_id):
            case_hash = hash(case_id) % case_split
            gene_hash = hash(gene_id) % gene_split
            return '{}-{}'.format(case_hash, gene_hash)

        routing_udf = udf(compute_routing, StringType())
        df = df.withColumn(
            colName=self.routing_column,
            col=routing_udf(df.case_id, df.gene_id))
