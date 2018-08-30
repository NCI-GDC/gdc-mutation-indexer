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

from exports.builders.utils import (
    standardize_schema,
)

logging.basicConfig()


class CNVCentricBuilder(BaseBuilder):
    """
    CNV: Copy Number Variation
    Builds cnv-centric dataframe given case, gene, and maf dataframes:

        cnv{}
        |____ consequence[]
        |             |_____ gene{}
        |____ occurrence[]
                      |_____ case{}
                                |____ observation[]

    """

    index_name = 'cnv_centric'
    id_field = 'cnv_id'

    def build(self, maf_df):
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric = self.get_existing()
            if self.cnv_centric is not None:
                return self

        # read from gistic
        cnv_df = GisticBuilder(self.config, self.sqlContext).build(maf_df)

        # Consequence
        cons_df = ConsequenceBuilder(self.config,
                                     self.sqlContext).build_for_cnv(cnv_df)

        # Occurrence
        occurrence_df = OccurrenceBuilder(
                            self.config,
                            self.sqlContext).build_for_cnv(cnv_df, maf_df)

        self.log('Final join CNV + Consequence + Occurrence')
        intermediate_df = cnv_df.join(cons_df, on='cnv_id', how='left')
        joined_cnv_df = intermediate_df.join(occurrence_df, on='cnv_id',
                                             how='right')

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
