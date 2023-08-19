import logging

from pyspark import sql

from mutation_indexer.builders.base_input_builder import BaseInputBuilder
from mutation_indexer.configuration.old_adapter import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class ClinicalAnnotationBuilder(BaseInputBuilder):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(self, config, sqlContext):
        super(ClinicalAnnotationBuilder, self).__init__(config, sqlContext, 'tsv')

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features
        """

        df = self.combine()

        return df

    def merge_with_maf(self, maf_df):
        pass

    def standardize_schema_with_maf(self, df, maf_df, field_not_none=None):
        pass
