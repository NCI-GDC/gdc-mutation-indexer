import logging

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    array,
    col,
    collect_list,
    explode,
    lit,
    struct,
    udf,
)

from exports.builders import (
    BaseBuilder,
    CaseBuilder,
    ConsequenceBuilder,
    ObservationBuilder,
)

from exports.builders.df_builders import (
    get_gene_df,
)

from exports.builders.utils import (
    uuid5_col,
)

from utils import standardize_schema

logging.basicConfig()

# region MoveMe


def melt(frame,
         id_vars,
         value_vars=None,
         var_name="variable",
         value_name="value"):
    """
    Source:
    https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe

    See also:
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.melt.html

    The opposite of pivoting a dataframe

    :param frame: input pyspark.DataFrame
    :param id_vars: Column(s) to use as identifier variables
    :type id_vars: Iterable
    :param value_vars: Column(s) to unpivot. If not specified, uses all columns
    that are not set as id_vars.
    :type value_vars: Iterable
    :param var_name: Name to use for the 'variable' column. If None, default to 'variable'.
    :param value_name: Name to use for the 'value' column. If none, default to 'value'.
    :return: long version of dataframe
    """

    # We assume id_vars is a strict subset of value_vars
    if not value_vars:
        value_vars = list(set(frame.columns) - set(id_vars))

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars
    ))

    _temp = frame.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x)
        for x in [var_name, value_name]
    ]

    return_df = _temp.select(*cols)

    return return_df

# endregion


class CNVCentricBuilder(BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case and maf dataframes::

        cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                    |_____ case{}
                                |____ observation[]

    TODO: this is kinda like mafBuilder, but also kinda like the index_builders.
    figure out what to do about that
    """

    # region Class-level Fields
    index_name = 'cnv_centric'
    id_field = 'cnv_id'
    # endregion

    # region Constructor

    def __init__(self, config, sqlContext):
        BaseBuilder.__init__(self, config, sqlContext)
        self._url = self.get_url(config)

    # endregion

    # region Abstract Overrides

    def build(self, maf_df):
        """
        Builds CNV Centric index

        # TODO: Load from gistic files
        # "real" file is in BRCA / all_thresholded.by_genes.txt
        # fake file is (-1).txt
        # gonna read it in a la maf.py
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric = self.get_existing()
            if self.cnv_centric is not None:
                return self

        # read from gistic
        initial_cnv_df = self.get_initial_cnv()

        # do data massaging
        massaged_cnv_df = self.massage_cnv_df(initial_cnv_df, maf_df)

        # do joins
        # joined_cnv_df = self.join_cnv(initial_cnv_df, maf_df)

        # truncate outliers
        # cnv_centric_df = self.truncate(joined_cnv_df)

        # warning: this will strip out anything that isn't
        # specified in the schema

        cleansed_df = standardize_schema(massaged_cnv_df, "cnv_centric", "cnv")

        cnv_centric_df = cleansed_df

        # save final df as property
        self.cnv_centric = cnv_centric_df

        ###############
        # LOGGING
        self.log_count(self.cnv_centric)
        self.log('Build finished')
        ###############

        # Check if we should write
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    # endregion

    # region Private Helper Functions

    def massage_cnv_df(self, initial_cnv_df, maf_df):

        # trim gene symbol of last .{dd}
        new_df = self.trim_gene_symbol(initial_cnv_df)

        new_df = self.remove_extra_columns(new_df)

        # melt
        # TODO: find a good home for this method,
        # it doesn't belong in this module
        # get column names
        new_df = melt(new_df,
                      id_vars=["gene_id"],
                      var_name="aliquot_id",
                      value_name="cnv_change")

        # join to gene df on trimmed gene symbol = gene_id
        # gene df from MAF builder or gene_centric df?
        new_df = self.join_to_gene(new_df, maf_df)

        # TODO: good renaming
        new_df = self.rename_cols(new_df)

        # add id
        new_df = self.add_id(new_df)

        # just add a column of true for now
        new_df = self.add_gene_level_cn(new_df)

        # ncbi_build ?
        new_df = self.add_ncbi_build(new_df)

        # do some crap for consequence / occurrence

        return new_df

    def remove_extra_columns(self, initial_cnv_df):
        """
        These columns aren't useful right now, and they confuse the melting
        """

        updated_df = initial_cnv_df.drop('Locus ID')
        updated_df = updated_df.drop('Cytoband')

        return updated_df

    def trim_gene_symbol(self, initial_cnv_df):
        """
        Gistic file includes something else
        We want to trim it.
        E.g., ENSG00000008128.21 should be ENSG00000008128
        Unfortunately there is no easy way to do this in place, 
        so we must add the trimmed column and remove the old column.
        """

        def trim_gene_symbol_inner(gene_id):
            period_location = gene_id.rfind('.')
            if period_location != -1:
                gene_id = gene_id[:period_location]

            return gene_id

        trim_gene_symbol_udf = udf(trim_gene_symbol_inner, StringType())
        trimmed_df = initial_cnv_df.withColumn('gene_id',
                                               trim_gene_symbol_udf(
                                                   col('Gene Symbol')
                                               ))

        trimmed_and_deduped_df = trimmed_df.drop('Gene Symbol')

        return trimmed_and_deduped_df

    def join_to_gene(self, initial_cnv_df, maf_df):
        """
        Get the other gene information
        """
        gene_df = get_gene_df(maf_df, index_name='gene_centric',
                              unique_fields=['gene_id'])

        ##############
        # LOGGING
        self.log('Joining gene with gistic [inner, "gene_id"]')
        ###############

        gistic_and_gene_df = (
            gene_df.join(initial_cnv_df,
                         gene_df.gene_id == initial_cnv_df.gene_id,
                         'inner')
                   .drop(initial_cnv_df.gene_id)
        )

        return gistic_and_gene_df

    def rename_cols(self, initial_cnv_df):

        # TODO: https://stackoverflow.com/questions/34077353/how-to-change-dataframe-column-names-in-pyspark
        # test the various ways of doing this to see what's fastest

        old_to_new = {'gene_chromosome': 'chromosome',
                      'gene_start': 'start_position',
                      'gene_end': 'end_position'}

        df = initial_cnv_df

        for old, new in old_to_new.items():
            df = df.withColumnRenamed(old, new)

        # renamed_cols_df = initial_cnv_df.select(col('gene_chromosome').alias('chromosome'),
        #                                        col('gene_start').alias('start_position'),
        #                                        col('gene_end').alias('end_position'))

        return df

    def add_id(self, initial_cnv_df):
        """
        Business key: chromosome, start_position, end_position, cnv_change
        """

        """
        TORI TODO:

        cnv_change = -2 to 2
        cnv = gene + cnv change pair
        load in gene? or is the gene info in the line?
        check against sample output doc

        """

        cnv_df_with_id = initial_cnv_df.withColumn('cnv_id', uuid5_col(
            col('chromosome'),
            col('start_position'),
            col('end_position'),
            col('cnv_change')
        ))

        return cnv_df_with_id

    def add_gene_level_cn(self, initial_cnv_df):

        cnv_df_with_gene_level_cn = \
            initial_cnv_df.withColumn('gene_level_cn', lit(True))

        return cnv_df_with_gene_level_cn

    def add_ncbi_build(self, initial_cnv_df):

        cnv_df_with_ncbi_build = \
            initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

        return cnv_df_with_ncbi_build

    def truncate(self, cnv_df_to_truncate):
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        truncated_df = self.truncate_df_at_percentile(cnv_df_to_truncate,
                                                      'occurrence',
                                                      threshold)

        return truncated_df

    def join_cnv(self, cnv_df, maf_df):

        cons_df = self.build_consequence(maf_df)
        occurrence_df = self.build_occurrence(maf_df)

        ##############
        # LOGGING
        self.log('Final join CNV + Consequence + Occurrence')
        ###############

        cnv_centric_df = cnv_df.join(cons_df, on='cnv_id')\
                               .join(occurrence_df, on='cnv_id')

        return cnv_centric_df

    def get_url(self, config):
        if config.gistic_url is not None:
            return config.gistic_url

        # TODO: what should this actually be?
        return "stuff from indexd most likely"

    def get_initial_cnv(self, url=None):
        """
        Inevitably there's crap in this gistic
        """

        if url is None:
            if self._url is not None:
                url = self._url
            else:
                self.logger.error("Url not passed, instance _urls not set")
                raise Exception("Url not specified to load CNV")

        # to return
        return_df = None

        try:
            new_df = self.read_gistic(url)

            # do some crap

            self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                           url))

            return_df = new_df
        except BaseException as e:
            self.logger.error(e)
            # TODO: reraise?

        assert return_df is not None
        # TODO: ?? self.df = return_df

        return return_df

    def read_gistic(self, url=None):
        """
        Reads in dataframes from a url?
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def build_consequence(self, maf_df):
        """
        I have no idea what these options mean...
        join_gene, add_gene_aa_change?
        TODO: this is going to need to be a whole different kind
        of consequence
        """
        cons_df = (ConsequenceBuilder(self.config, self.sqlContext)
                   .build(input_df=maf_df,
                          index_name=self.index_name,
                          join_gene=True,
                          add_gene_aa_change=True))
        return cons_df

    def build_occurrence(self, maf_df):
        """
        Occurrence
        """
        # 1. Observation
        obs_df = self.build_observation(maf_df)

        # 2. Case
        case_df = self.build_case(maf_df)

        # 3. Join Case to Observation
        ###############
        # LOGGING
        self.log('Joining Cases with Observation, [right, case_id]')
        ###############

        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('ssm_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('ssm_id')
                         .agg(collect_list('occurrence').alias('occurrence')))

        ###############
        # LOGGING
        self.log_count(occurrence_df)
        ###############

        return occurrence_df

    def build_observation(self, maf_df):
        """
        Observation
        """
        ###############
        # LOGGING
        self.log('Aggregating Observation from MAF')
        ###############

        obs_df = ObservationBuilder().build(maf_df, self.index_name)
        return obs_df

    def build_case(self, maf_df):
        """
        Case
        """
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        ###############
        # LOGGING
        self.log_count(case_df)
        ###############

        return case_df

    # endregion
