import json
import logging

from pyspark import sql
from pyspark.sql import functions as F

import config
from exports import es_utils
from exports.builders import base_input_builder, utils

logging.basicConfig(format=config.LOG_FORMAT)


AVAILABLE_VARIATION_DATA = "available_variation_data"


class CaseBuilder(base_input_builder.BaseInputBuilder):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        super().__init__(config, sqlContext, "case")

        self._es_dataframe_util = es_dataframe_util

    def build_from_scratch(
        self,
        maf_metadata_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Builds Case dataframe
        """
        df = self.load_into_df(maf_metadata_df, ascat_df)

        # Select only columns that are in case mapping:
        df = utils.standardize_schema(df, "case_centric", "case")

        return df

    def load_into_df(
        self,
        maf_metadata_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        Loads case docs from the gdc_from_graph index into a dataframe
        """

        # Only load cases from the requested projects
        if self.config.projects:
            query = {"query": {"terms": {"project.project_id": self.config.projects}}}
        else:
            query = {"query": {"match_all": {}}}

        # Only retrieve the fields we want
        self.logger.info("Exclude fields: {}".format(self.config.exclude_fields))

        # Load cases from graph index
        df = self._es_dataframe_util.get_dataframe(
            es_utils.Index.Case,
            exclude_fields=self.config.exclude_fields,
            query=query,
        )

        # Get all the cases that have been tested for ssm
        all_maf_cases_df = maf_metadata_df.select("case_id")

        maf_and_ascat_df = self.populate_available_variation_data(
            all_maf_cases_df, ascat_df
        )

        df = df.join(maf_and_ascat_df, on=["case_id"], how="left")

        self.logger.info("Repartitioning case dataframe")
        df = df.repartition(self.config.df_repartition, "case_id")

        # TODO: DEV-1131 Move this functionality into the base class
        if self.config.cache_dataframes["cases"]:
            self.logger.info("Caching repartitioned case dataframe")
            df = df.cache()

        return df

    def populate_available_variation_data(
        self,
        all_maf_cases_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        Calculates the value of available_variation_data. The values in the
        available_variation_data are determined by the existance of the case_id
        in either or both of the ascat_df or the all_maf_cases_df. If the case_id
        is found in the ascat_df then "cnv" is added to the available_variation_data
        and if it is found in the all_maf_cases_df then "ssm" is added.

        Args:
            all_maf_cases_df: A data frame containing all case ids related to the maf
                data loaded in the build process
            ascat_df: A data frame of all the ascat data including all case ids related
                to the data.

        Returns:
            A data frame with cases and their associated available_variation_data

            df {}
            |---case_id
            +---available_variation_data
        """
        # Set all cases in maf_data to "tested"
        # i.e., 'available_variation_data' == 'ssm'
        maf_data_df = all_maf_cases_df.withColumn(
            AVAILABLE_VARIATION_DATA, F.lit("ssm")
        )

        # Stack with ascat data
        maf_and_ascat_df = maf_data_df.union(
            ascat_df.select("case_id", AVAILABLE_VARIATION_DATA)
        )

        # Finally, group by case
        maf_and_ascat_df = maf_and_ascat_df.groupby("case_id").agg(
            F.collect_set(AVAILABLE_VARIATION_DATA).alias(AVAILABLE_VARIATION_DATA)
        )

        return maf_and_ascat_df
