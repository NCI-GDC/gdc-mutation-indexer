import json
import logging

from pyspark.sql import functions as F
from pyspark import sql
from exports import es_utils

from exports.builders import utils

import config

logging.basicConfig(format=config.LOG_FORMAT)


class CaseBuilder:
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
    ):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext
        self._es_dataframe_util = es_dataframe_util

    def build(self, maf_df, ascat_df):
        """
        Builds Case dataframe
        """
        df = self.load_into_df(maf_df, ascat_df)

        # Select only columns that are in case mapping:
        df = utils.standardize_schema(df, "case_centric", "case")

        return df

    def load_into_df(
        self, maf_df: sql.DataFrame, ascat_df: sql.DataFrame
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
        exclude_fields = tuple(self.config.exclude_fields)
        self.logger.info("Exclude fields: {}".format(self.config.exclude_fields))

        df = self._es_dataframe_util.get_dataframe(
            es_utils.Index.Case,
            exclude_fields=exclude_fields,
            query=query,
        )

        # Get all the cases that have been tested for ssm
        # (from aliquots in maf_df headers)
        all_maf_cases_df = utils.get_case_ids_from_source_es(
            self.config, self.sqlContext
        )

        maf_and_ascat_df = self.populate_available_variation_data(
            maf_df, all_maf_cases_df, ascat_df
        )

        df = df.join(maf_and_ascat_df, on=["case_id"], how="left")

        self.logger.info("Repartitioning case dataframe")
        df = df.repartition(self.config.df_repartition, "case_id")

        if self.config.cache_dataframes["cases"]:
            self.logger.info("Caching repartitioned case dataframe")
            df.cache().count()

        return df

    def populate_available_variation_data(
        self,
        maf_df: sql.DataFrame,
        all_maf_cases_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        This function calculates the value of the column
        "available_variation_data."

        We retrieve a set of cases from graph_index
        and add "ssm" for both those cases and the cases in the maf_df,
        "cnv" if that case id is present in the ascat_df,
        ["ssm", "cnv"] if both.

        """

        avd = "available_variation_data"

        # Get set of "tested cases" from maf_df
        maf_data_df = maf_df.select("case_id", avd).dropDuplicates(
            subset=["case_id", avd]
        )

        # Add empty rows to input_data corresponding to "empty cases"
        maf_data_df = all_maf_cases_df.join(maf_data_df, on=["case_id"], how="left")

        # the original maf_data is in array form ['ssm'] and we need 'ssm'
        maf_data_df = maf_data_df.drop(avd)
        # Set all cases in maf_data to "tested"
        # i.e., 'available_variation_data' == 'ssm'
        maf_data_df = maf_data_df.withColumn(avd, F.lit("ssm"))

        # Stack with ascat data
        maf_and_ascat_df = maf_data_df.union((ascat_df.select("case_id", avd)))

        # Finally, group by case
        maf_and_ascat_df = maf_and_ascat_df.groupby("case_id").agg(
            F.collect_set(avd).alias(avd)
        )

        return maf_and_ascat_df
