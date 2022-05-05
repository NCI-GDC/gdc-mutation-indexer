import io
import json
import logging

from pyspark.sql.functions import collect_set, lit
import yaml

from exports.builders.utils import get_case_ids_from_source_es, standardize_schema

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CaseBuilder(object):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self, maf_df, ascat_df):
        """
        Builds Case dataframe
        """
        df = self.load_into_df(maf_df, ascat_df)

        # Select only columns that are in case mapping:
        df = standardize_schema(df, "case_centric", "case")

        return df

    def load_into_df(self, maf_df, ascat_df):
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

        s = io.StringIO()
        yaml.safe_dump(df.schema.json(), s)

        self.logger.info(s.getvalue())

        # Get all the cases that have been tested for ssm
        # (from aliquots in maf_df headers)
        all_maf_cases = get_case_ids_from_source_es(self.config, self.sqlContext)

        maf_and_ascat_df = self.populate_available_variation_data(
            maf_df, all_maf_cases, ascat_df
        )

        df = df.join(maf_and_ascat_df, on=["case_id"], how="left")

        self.logger.info("Repartitioning case dataframe")
        df = df.repartition(self.config.df_repartition, "case_id")

        if self.config.cache_dataframes["cases"]:
            self.logger.info("Caching repartitioned case dataframe")
            df.cache().count()

        return df

    def populate_available_variation_data(self, maf_df, all_maf_cases_df, ascat_df):
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
        maf_data_df = maf_data_df.withColumn(avd, lit("ssm"))

        # Stack with ascat data
        maf_and_ascat_df = maf_data_df.union((ascat_df.select("case_id", avd)))

        # Finally, group by case
        maf_and_ascat_df = maf_and_ascat_df.groupby("case_id").agg(
            collect_set(avd).alias(avd)
        )

        return maf_and_ascat_df
