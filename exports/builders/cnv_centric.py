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
    melt_df,
    uuid5_col,
    standardize_schema,
)

logging.basicConfig()


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

    index_name = 'cnv_centric'
    id_field = 'cnv_id'

    def __init__(self, config, sqlContext):
        BaseBuilder.__init__(self, config, sqlContext)
        self._url = self._get_url(config)

        # TODO: TEMP
        temp_file_path = self.config.input_dir + "/temp_aliquot_to_case.txt"
        mapping = {}

        # TODO: get this for real
        with open(temp_file_path) as f:
            # read into dictionary
            for line in f:
                (k, v) = line.split()
                mapping[k] = v

        self._aliquot_id_to_case_id_map = mapping

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
        initial_cnv_df = self._get_initial_cnv()

        # do data massaging
        massaged_cnv_df = self._massage_cnv_df(initial_cnv_df, maf_df)

        # do joins
        joined_cnv_df = self._join_consequence_and_occurrence(massaged_cnv_df, maf_df)

        # truncate outliers
        cnv_centric_df = self._truncate(joined_cnv_df)

        # warning: this will strip out anything that isn't
        # specified in the schema
        cleansed_df = standardize_schema(joined_cnv_df, "cnv_centric", "cnv")

        cnv_centric_df = cleansed_df

        # save final df as property
        self.cnv_centric = cnv_centric_df

        self.log_count(self.cnv_centric)
        self.log('Build finished')

        # Check if we should write
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    def _massage_cnv_df(self, initial_cnv_df, maf_df):

        # trim gene symbol of last .{dd}
        new_df = self._trim_gene_symbol(initial_cnv_df)

        new_df = self._remove_extra_columns(new_df)

        # melt dataframe
        # TODO: find a good home for this method,
        # it doesn't belong in this module
        # get column names
        new_df = melt_df(new_df,
                         id_vars=["gene_id"],
                         var_name="aliquot_id",
                         value_name="cnv_change")

        # import ipdb; ipdb.set_trace()
        # new_df = new_df.groupby(["gene_id", "cnv_change"]).agg(collect_list("aliquot_id"))

        # join to gene df on trimmed gene symbol = gene_id
        # gene df from MAF builder or gene_centric df?
        new_df = self._join_to_gene(new_df, maf_df)

        # TODO: good renaming
        new_df = self._rename_cols(new_df)

        # add id
        new_df = self._add_id(new_df)

        new_df = self._add_gene_level_cn(new_df)

        new_df = self._add_ncbi_build(new_df)

        return new_df

    def _remove_extra_columns(self, initial_cnv_df):
        """
        These columns aren't useful right now, and they confuse the melting
        """

        updated_df = initial_cnv_df.drop('Locus ID')
        updated_df = updated_df.drop('Cytoband')

        return updated_df

    def _trim_gene_symbol(self, initial_cnv_df):
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

    def _join_to_gene(self, initial_cnv_df, maf_df):
        """
        Get the other gene information
        """
        gene_df = get_gene_df(maf_df, index_name='gene_centric',
                              unique_fields=['gene_id'])

        self.log('Joining gene with gistic [inner, "gene_id"]')

        gistic_and_gene_df = (
            gene_df.join(initial_cnv_df,
                         gene_df.gene_id == initial_cnv_df.gene_id,
                         'inner')
                   .drop(initial_cnv_df.gene_id)
        )

        return gistic_and_gene_df

    def _rename_cols(self, initial_cnv_df):

        # TODO: https://stackoverflow.com/questions/34077353/how-to-change-dataframe-column-names-in-pyspark
        # test the various ways of doing this to see what's fastest

        old_to_new = {'gene_chromosome': 'chromosome',
                      'gene_start': 'start_position',
                      'gene_end': 'end_position'}

        df = initial_cnv_df

        for old, new in old_to_new.items():
            df = df.withColumnRenamed(old, new)

        return df

    def _add_id(self, initial_cnv_df):
        """
        Business key: chromosome, start_position, end_position, cnv_change
        """

        cnv_df_with_id = initial_cnv_df.withColumn('cnv_id', uuid5_col(
            col('chromosome'),
            col('start_position'),
            col('end_position'),
            col('cnv_change')
        ))

        return cnv_df_with_id

    def _add_gene_level_cn(self, initial_cnv_df):

        cnv_df_with_gene_level_cn = \
            initial_cnv_df.withColumn('gene_level_cn', lit(True))

        return cnv_df_with_gene_level_cn

    def _add_ncbi_build(self, initial_cnv_df):

        cnv_df_with_ncbi_build = \
            initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

        return cnv_df_with_ncbi_build

    def _truncate(self, cnv_df_to_truncate):
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        truncated_df = self.truncate_df_at_percentile(cnv_df_to_truncate,
                                                      'occurrence',
                                                      threshold)

        return truncated_df

    def _join_consequence_and_occurrence(self, cnv_df, maf_df):

        cons_df = self._build_consequence(cnv_df)
        occurrence_df = self._build_occurrence(cnv_df, maf_df)

        self.log('Final join CNV + Consequence + Occurrence')

        # TODO: how to join?
        test = cnv_df.join(cons_df, on='cnv_id', how='left')

        cnv_centric_df = test.join(occurrence_df, on='cnv_id', how='right')

        return cnv_centric_df

    def _get_url(self, config):
        if config.gistic_url is not None:
            return config.gistic_url

        # TODO: what should this actually be?
        return "stuff from indexd most likely"

    def _get_initial_cnv(self, url=None):
        """
        Read gistic into dataframe. 
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
            new_df = self._read_gistic(url)

            self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                           url))

            return_df = new_df
        except BaseException as e:
            self.logger.error(e)
            # TODO: reraise?

        assert return_df is not None
        # TODO: ?? self.df = return_df

        return return_df

    def _read_gistic(self, url=None):
        """
        Reads in tab-delimited file into dataframe
        TODO: put in utils
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def _build_consequence(self, cnv_df):
        """
        For now this is just gene information
        """

        # Add consequence_id
        id_added_df = cnv_df.withColumn('consequence_id', uuid5_col(
            col('symbol'),
            col('gene_id'),
            col('is_cancer_gene_census'),
            col('biotype')
        ))

        # Create gene structure
        cons_df = id_added_df.select('cnv_id',
                                     struct(
                                        'consequence_id',
                                        struct(
                                            'symbol',
                                            'gene_id',
                                            'is_cancer_gene_census',
                                            'biotype'
                                        ).alias('gene'))
                                     .alias('consequence'))

        return cons_df

    def _build_occurrence(self, cnv_df, maf_df):
        """
        Occurrence
        """
        # 1. Observation
        obs_df = self._build_observation(cnv_df, maf_df)

        # 2. Case
        case_df = self._build_case(maf_df)

        # 3. Join Case to Observation
        self.log('Joining Cases with Observation, [right, case_id]')

        occurrence_df = (case_df.join(obs_df, on=['case_id'], how='right')
                         .select('cnv_id',
                                 struct('occurrence_id',
                                        struct('observation',
                                               *case_df.columns).alias('case'))
                                 .alias('occurrence'))
                         .groupby('cnv_id')
                         .agg(collect_list('occurrence').alias('occurrence')))

        self.log_count(occurrence_df)

        return occurrence_df

    def _build_observation(self, initial_cnv_df, maf_df):
        """
        Observation
        """
        self.log('Aggregating Observation from gistic')

        mapping = self._aliquot_id_to_case_id_map

        def add_case_id(aliquot_id):
            return mapping[aliquot_id]

        case_id_udf = udf(add_case_id)

        # add case id
        new_df = initial_cnv_df.withColumn('case_id',
                                           case_id_udf(
                                               col('aliquot_id')
                                           ))

        # add occurrence id
        new_df = new_df.withColumn('occurrence_id',
                                   uuid5_col(col('cnv_id'),
                                             col('case_id')))

        # add observation id
        new_df = new_df.withColumn('observation_id',
                                   uuid5_col(col('cnv_id'),
                                             col('case_id'),
                                             col('aliquot_id')))

        # observation structure, TODO: add more fields
        obs_df = (new_df.select('cnv_id',
                                'case_id',
                                'occurrence_id',
                                struct('observation_id').alias('observation'))
                        .groupby('cnv_id', 'case_id', 'occurrence_id')
                        .agg(collect_list('observation').alias('observation')))

        return obs_df

    def _get_case_id_from_aliquot_id(self, aliquot_id):
        """
        TODO: replace with call to gdc_from_graph, most likely
        """

        try:
            return_id = self._aliquot_id_to_case_id_map[aliquot_id]
        except KeyError:
            self.log('Aliquot to case id mapping failure')
        else:
            return return_id

    def _build_case(self, maf_df):
        """
        Case
        """
        case_df = CaseBuilder(self.config, self.sqlContext).build(maf_df)

        self.log_count(case_df)

        return case_df
