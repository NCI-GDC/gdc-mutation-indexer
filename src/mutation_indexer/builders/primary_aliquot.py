from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import TypedDict

from mutation_indexer import es_utils, schemas
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build


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


class PrimaryAliquotInputs(TypedDict):
    pass


class PrimaryAliquotBuilder(
    bases.PrimaryAliquotBuilder[viz.Builder, PrimaryAliquotInputs]
):
    __slots__ = ("_es_rdd_util",)

    FILE_URL_BATCH_SIZE = 1000

    def __init__(
        self,
        config: viz.Builder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sql_context: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
            es_rdd_util: The util for creating RDD objects from data in elasticsearch
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util=es_dataframe_util,
            input_type=PrimaryAliquotInputs,
            output=build.DataFrame.PRIMARY_ALIQUOT,
            additional_selections=("experimental_strategy",),
        )
        self._es_rdd_util = es_rdd_util

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

        if self._config.projects:
            project_clause = {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self._config.projects}
                    },
                }
            }

            query["query"]["bool"]["must"].append(project_clause)

        aliquot_df = _expand_aliquots(
            self._es_rdd_util.get_rdd(
                build.IndexType.FILE, include_fields=included_fields, query=query
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

    def _build_from_scratch(self, input_dfs: PrimaryAliquotInputs) -> sql.DataFrame:
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---aliquot_id
            |---case_id
            |---entity
            |---entity_id
            |---experimental_strategy
            +---file_id
        """
        filters = (
            [
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {"cases.project.project_id": self._config.projects}
                        },
                    }
                }
            ]
            if self._config.projects
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
                "aliquot_id",
                "case_id",
                "entity",
                "entity_id",
                "experimental_strategy",
                "file_id",
            )
        )
