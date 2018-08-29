import logging

from pyspark.sql.functions import (
    col,
    udf,
)

from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    GisticBuilder,
    OccurrenceBuilder,
)

from exports.builders.df_builders import (
    get_gene_df,
)

from exports.builders.utils import (
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

    """

    index_name = 'cnv_centric'
    id_field = 'cnv_id'
    old_to_new = {'gene_chromosome': 'chromosome',
                  'gene_start': 'start_position',
                  'gene_end': 'end_position'}

    def __init__(self, config, sqlContext):
        BaseBuilder.__init__(self, config, sqlContext)

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

    # region Abstract Overrides

    def build(self, maf_df):
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric = self.get_existing()
            if self.cnv_centric is not None:
                return self

        import ipdb; ipdb.set_trace()
        # read from gistic
        gistic_df = GisticBuilder(self.config, self.sqlContext).build()

        # add gene information
        cnv_df = self._add_gene_information(gistic_df, maf_df)

        # Consequence
        cons_df = ConsequenceBuilder(self.config,
                                     self.sqlContext).build_for_cnv(cnv_df)

        # add case id, needed for occurrence builder
        cnv_df = self._add_case_id(cnv_df)

        # Occurrence
        occurrence_df = OccurrenceBuilder(self.config,
                                          self.sqlContext).build_for_cnv(cnv_df, maf_df)

        self.log('Final join CNV + Consequence + Occurrence')
        intermediate_df = cnv_df.join(cons_df, on='cnv_id', how='left')
        joined_cnv_df = intermediate_df.join(occurrence_df, on='cnv_id', how='right')

        # truncate outliers
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        truncated_df = self.truncate_df_at_percentile(joined_cnv_df,
                                                      'occurrence',
                                                      threshold)

        # warning: this will strip out anything that isn't
        # specified in the schema
        cleansed_df = standardize_schema(truncated_df, "cnv_centric", "cnv")

        # save final df as property
        self.cnv_centric = cleansed_df

        self.log_count(self.cnv_centric)
        self.log('Build finished')

        # Check if we should write
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

    # endregion

    def _add_gene_information(self, initial_cnv_df, maf_df):

        # join to gene df on trimmed gene symbol = gene_id
        new_df = self._join_to_gene(initial_cnv_df, maf_df)

        # gene information is required to create cnv_id
        new_df = self._add_id(new_df)

        return new_df

    def _join_to_gene(self, initial_cnv_df, maf_df):
        """
        Get the other gene information
        """
        gene_df = get_gene_df(maf_df, index_name='gene_centric',
                              unique_fields=['gene_id'])

        self.log('Joining gene with gistic [inner, "gene_id"]')
        df = (
            gene_df.join(initial_cnv_df,
                         gene_df.gene_id == initial_cnv_df.gene_id,
                         'inner')
                   .drop(initial_cnv_df.gene_id)
        )

        # rename columns from gene names to cnv names
        for old, new in CNVCentricBuilder.old_to_new.items():
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

    def _get_case_id_from_aliquot_id(self, aliquot_id):
        """
        TODO: replace with call to gdc_from_graph, most likely
        """

        try:
            return_id = self._aliquot_id_to_case_id_map[aliquot_id]
        except KeyError:
            ###############
            # LOGGING
            self.log('Aliquot to case id mapping failure')
            ###############
        else:
            return return_id

    def _add_case_id(self, cnv_df):
        mapping = self._aliquot_id_to_case_id_map

        def add_case_id_inner(aliquot_id):
            return mapping[aliquot_id]

        case_id_udf = udf(add_case_id_inner)

        # add case id
        new_df = cnv_df.withColumn('case_id',
                                   case_id_udf(col('aliquot_id')))

        return new_df
