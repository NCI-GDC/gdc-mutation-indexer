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
from pyspark.sql import functions as F

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build


class CaseInputs(TypedDict):
    expression_value_df: sql.DataFrame


class CaseBuilder(bases.InputBuilder[gene_expression.CaseBuilder, CaseInputs]):
    __slots__ = ("_s3_client",)

    def __init__(
        self,
        config: gene_expression.CaseBuilder,
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
            .groupBy(F.lit(1))
            .agg(F.sort_array(F.collect_set("case_id")).alias("cases"))
            .select("cases")
        )
