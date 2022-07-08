import json
import logging

from pyspark import sql
from pyspark.sql import functions as F

import config
from exports.builders import base_input_builder, utils

logging.basicConfig(format=config.LOG_FORMAT)


AVAILABLE_VARIATION_DATA = "available_variation_data"


class CaseBuilder(base_input_builder.BaseInputBuilder):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(self, config: config.BaseConfig, sqlContext: sql.SQLContext) -> None:
        super().__init__(config, sqlContext, "case")

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
            query = json.dumps(
                {"query": {"terms": {"project.project_id": self.config.projects}}}
            )
        else:
            query = json.dumps({"query": {"match_all": {}}})

        # Only retrieve the fields we want
        self.logger.info("Exclude fields: {}".format(self.config.exclude_fields))

        # Load cases from graph index
        # TODO: DEV-1132 Switch to use the es_utils.DataFrameUtil
        if self.config.graph_case_doc_type:
            es_source = "{}/{}".format(
                self.config.graph_case_index, self.config.graph_case_doc_type
            )
        else:
            es_source = self.config.graph_case_index

        df = (
            self.sqlContext.read.format("es")
            .option("es.nodes", self.config.source_es_nodes)
            .option("es.net.http.auth.user", self.config.source_es_user)
            .option("es.net.http.auth.pass", self.config.source_es_pass)
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", self.config.es_use_ssl)
            .option(
                "es.net.ssl.cert.allow.self.signed", self.config.disable_es_verify_certs
            )
            .option("es.nodes.resolve.hostname", "false")
            .option("es.query", query)
            .option("es.read.field.exclude", ",".join(self.config.exclude_fields))
            .option("es.resource.read", es_source)
            .load(es_source)
        )

        # Get all the cases that have been tested for ssm
        all_maf_cases_df = maf_metadata_df.select("case_id")

        maf_and_ascat_df = self.populate_available_variation_data(
            all_maf_cases_df, ascat_df
        )

        df = df.join(maf_and_ascat_df, on=["case_id"], how="left")

        self.logger.info("Repartitioning case dataframe")
        df = df.repartition(self.config.df_repartition, "case_id")

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
