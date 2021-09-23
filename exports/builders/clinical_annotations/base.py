import logging

from config import LOG_FORMAT
from exports.builders.base_input_builder import BaseInputBuilder
from pyspark import sql

logging.basicConfig(format=LOG_FORMAT)


class ClinicalAnnotationBuilder(BaseInputBuilder):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(self, config, spark_session: sql.SparkSession):
        super().__init__(config, spark_session, 'tsv')

    def build_from_scratch(self):
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
