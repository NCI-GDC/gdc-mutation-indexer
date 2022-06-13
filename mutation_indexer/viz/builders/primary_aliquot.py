import logging

from indexclient import client
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.core import configuration
from mutation_indexer.driver import es_utils
from mutation_indexer.driver.builders import bases
from mutation_indexer.viz import schemas


def _expand_aliquots(aliquot_df: sql.DataFrame) -> sql.DataFrame:
    return (
        aliquot_df.select("file_id", F.explode_outer("cases").alias("case"))
        .select(
            "file_id",
            F.col("case.case_id").alias("case_id"),
            F.explode_outer("case.samples").alias("sample"),
        )
        .select(
            "file_id",
            "case_id",
            F.col("sample.sample_id").alias("sample_id"),
            F.explode_outer("sample.portions").alias("portion"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("portion.analytes").alias("analyte"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("analyte.aliquots").alias("aliquot"),
        )
    )


class PrimaryAliquotBuilder(bases.PrimaryAliquotBuilder):
    FILE_URL_BATCH_SIZE = 1000

    def __init__(
        self,
        config: configuration.ConfigAdapter,
        sql_context: sql.SQLContext,
        indexd: client.IndexClient,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sql_context: The sql context object for the current pyspark run
            indexd: The indexd client for retrieving documents
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
            es_rdd_util: The util for creating RDD objects from data in elasticsearch
        """
        super().__init__(
            config,
            sql_context,
            es_dataframe_util,
            "primary_aliquot",
            additional_selections=("experimental_strategy",),
        )
        self._sql_context = sql_context
        self._indexd = indexd
        self._es_rdd_util = es_rdd_util
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_aliquot_level_df(self) -> sql.DataFrame:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "nested": {
                                "path": "cases.samples.portions.analytes.aliquots",
                                "query": {
                                    "exists": {
                                        "field": "cases.samples.portions.analytes.aliquots"
                                    }
                                },
                            }
                        },
                    ]
                }
            }
        }
        included_fields = (
            "file_id",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
            "cases.samples.portions.analytes.aliquots.created_datetime",
        )
        aliquot_data_schema = schemas.load_schema(
            "builders/primary_aliquot/aliquot_data.json"
        )

        if self.config.projects:
            project_clause = {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self.config.projects}
                    },
                }
            }

            query["query"]["bool"]["must"].append(project_clause)

        aliquot_df = _expand_aliquots(
            self._es_rdd_util.get_rdd(
                es_utils.Index.File, include_fields=included_fields, query=query
            )
            .toDF(aliquot_data_schema)
            .select("_source.*")
        ).select(
            "file_id",
            "case_id",
            "sample_id",
            "aliquot.aliquot_id",
            F.col("aliquot.created_datetime")
            .cast("timestamp")
            .alias("aliquot_created_datetime"),
        )

        return aliquot_df

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---case_id
            |---file_id
            |---experimental_strategy
        """
        filters = (
            [
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {"cases.project.project_id": self.config.projects}
                        },
                    }
                }
            ]
            if self.config.projects
            else [{"match_all": {}}]
        )

        include_fields = (
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.sample_type",
        )
        primary_aliquot_df = self._get_primary_aliquot_df(
            filters, include_fields=include_fields
        ).select(
            "entity_id",
            "entity",
            "case_id",
            "file_id",
            "experimental_strategy",
            "sample_id",
        )
        aliquot_data_df = self._get_aliquot_level_df()
        aliquot_df = primary_aliquot_df.join(
            aliquot_data_df, on=["file_id", "case_id", "sample_id"], how="left"
        )

        aliquot_window = (
            sql.Window()
            .partitionBy("entity", "entity_id")
            .orderBy("aliquot_created_datetime", "aliquot_id")
        )

        return (
            aliquot_df.withColumn("row_number", F.row_number().over(aliquot_window))
            .where(F.col("row_number") == 1)
            .select(
                "entity_id",
                "entity",
                "case_id",
                "file_id",
                "experimental_strategy",
                "aliquot_id",
            )
        )
