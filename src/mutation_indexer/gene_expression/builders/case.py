"""A module for functionality related to the building of the gene expression case data.

For documentation on the formatting of the data please refer to the GE service.
https://github.com/NCI-GDC/gene-expression/blob/main/README.md#data-cache
"""

import io
import marshal
from typing import TypedDict

import more_itertools
import mypy_boto3_s3 as s3
from pyspark import sql
from pyspark.sql import functions as pyspark_functions

from mutation_indexer import builders
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite
from mutation_indexer.gene_expression import configuration


class CaseInputs(TypedDict):
    expression_value_df: sql.DataFrame


class CaseBuilder(builders.InputBuilder[configuration.CaseBuilder, CaseInputs]):
    __slots__ = ("_s3_client",)

    def __init__(
        self,
        config: configuration.CaseBuilder,
        spark_session: sql.SparkSession,
        s3_client: s3.Client,
    ) -> None:
        """A builder for constructing the case data used by the gene expression service.

        Args:
            config: The configuration for building and uploading the case data.
            spark_session: The session associated with the current run of spark.
            s3_client: The client used to upload the case data to s3.
        """
        super().__init__(
            config,
            spark_session,
            input_type=CaseInputs,
            output=build.DataFrame.CASE,
        )

        self._s3_client = s3_client

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        """Writes the case data using marshal and uploads to s3."""
        df = super()._write(df)
        row = more_itertools.one(df.collect())
        data = marshal.dumps(row.cases)

        with io.BytesIO(data) as b:
            self._s3_client.upload_fileobj(
                b,
                Bucket=self._config.destination.bucket,
                Key=self._config.destination.key,
            )

        return df

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        """Builds gene expression case data.

        case {}
        +---cases [str]  # sorted list of all unique case ids.
        """
        return (
            input_dfs["expression_value_df"]
            .select("case_id")
            .groupBy(pyspark_functions.lit(1))
            .agg(
                pyspark_functions.sort_array(pyspark_functions.collect_set("case_id")).alias(
                    "cases"
                )
            )
            .select("cases")
        )


class CaseSQLInputs(TypedDict):
    expression_value_df: sql.DataFrame


class CaseSQLBuilder(builders.SQLiteBuilder[configuration.CaseSQLBuilder, CaseSQLInputs]):
    def __init__(
        self,
        config: configuration.CaseSQLBuilder,
        spark_session: sql.SparkSession,
        database: sqlite.SQLiteDatabase,
    ) -> None:
        """A builder for constructing and writing the case data for the GE SQLite DB.

        Args:
            config: The configuration for running this builder provided at runtime.
            spark_session: The spark session for the current run of the mutation
                indexer.
            database: The SQLite database to which the final data should be written.
        """
        super().__init__(
            config,
            spark_session,
            database,
            input_type=CaseSQLInputs,
            output=build.DataFrame.CASE_SQL,
        )

    @property
    def _create(self) -> str:
        return "CREATE TABLE cases(case_id TEXT PRIMARY KEY, submitter_id TEXT)"

    @property
    def _insert(self) -> str:
        return "INSERT INTO cases (case_id, submitter_id) VALUES (?, ?)"

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        """Builds a dataframe representing the case data for the GE database.

        Args:
            input_dfs: The required input data frames for building the data. Dee the
                CaseInputs class for more details.

        Returns:
            case_sql_df:
            |--- case_id
            +--- submitter_id
        """
        return input_dfs["expression_value_df"].select("case_id", "submitter_id").distinct()
