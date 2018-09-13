import logging

from pyspark.sql.functions import (
    col,
    udf,
)

from exports.builders import (
    BaseBuilder,
    ConsequenceBuilder,
    OccurrenceBuilder,
)

from exports.builders.utils import (
    standardize_schema,
    struct_select,
)

from exports.builders.df_builders import build_cnv_subtree

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

    def build(self, maf_df, gistic_df):
        """
        Builds CNV Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.index_use_existing:
            self.cnv_centric = self.get_existing()
            if self.cnv_centric is not None:
                return self

        # Consequence
        cons_df = (
            ConsequenceBuilder(self.config, self.sqlContext)
                .build_for_cnv(gistic_df)
        )

        # CNV
        cnv_df = build_cnv_subtree(gistic_df, cons_df, self.index_name,
                                   obs_df=None, add_fields=[])

        # Occurrence
        occurrence_df = (
            OccurrenceBuilder(self.config, self.sqlContext)
                .build_for_cnv(gistic_df, maf_df)
        )

        self.log('Final join CNV + Consequence + Occurrence')
        cnv_cons_df = cnv_df.join(cons_df, on='cnv_id', how='left')
        cnv_centric_df = cnv_cons_df.join(occurrence_df, on='cnv_id', how='left')

        # truncate outliers
        threshold = self.config.percentile_threshold['occurrences_per_cnv']
        cnv_centric_df = self.truncate_df_at_percentile(cnv_centric_df,
                                                        'occurrence',
                                                        threshold)

        # save final df as property
        self.cnv_centric = cnv_centric_df

        self.log_count(self.cnv_centric)
        self.log('Build finished')

        # Check if we should write
        if self.config.index_keep:
            self.write(self.config.index_paths[self.index_name])

        return self

