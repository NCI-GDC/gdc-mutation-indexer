"""A module for supporting the building of the binary data utilized by the GE service."""

import io
from collections.abc import Iterable
from typing import TypedDict

import mypy_boto3_s3 as s3
import numpy
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build


class BinaryInputs(TypedDict):
    expression_value_df: sql.DataFrame


class BinaryBuilder(bases.InputBuilder[gene_expression.BinaryBuilder, BinaryInputs]):
    __slots__ = ("_s3_client",)

    def __init__(
        self,
        config: gene_expression.BinaryBuilder,
        spark_session: sql.SparkSession,
        s3_client: s3.Client,
    ) -> None:
        """A builder for creating the binary files which store numpy GE data for the API.

        Args:
            config: The configuration for the binary builder.
            spark_session: The spark session associated with the run of the build.
            s3_client: The s3 client used for uploading the binary files to s3.
        """
        super().__init__(
            config,
            spark_session,
            input_type=BinaryInputs,
            output=build.DataFrame.BINARY,
        )

        self._s3_client = s3_client

    def _write_binary(self, key: str, values: Iterable[float]) -> None:
        data = numpy.array(values, dtype=numpy.float32).tobytes()

        with io.BytesIO(data) as b:
            self._s3_client.upload_fileobj(b, Bucket=self._config.bucket, Key=key)

    def _write_log2_uqfpkm(self, gene_id: str, values: Iterable[float]) -> None:
        key = self._config.log2_uqfpkm_key.format(gene_id=gene_id)

        self._write_binary(key, values)

    def _write_uqfpkm(self, gene_id: str, values: Iterable[float]) -> None:
        key = self._config.uqfpkm_key.format(gene_id=gene_id)

        self._write_binary(key, values)

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        df = super()._write(df)
        rows = df.select("gene_id", "log2_uqfpkm", "uqfpkm").toLocalIterator()

        for gene_id, log2_uqfpkm, uqfpkm in rows:
            self._write_log2_uqfpkm(gene_id, log2_uqfpkm)
            self._write_uqfpkm(gene_id, uqfpkm)

        return df

    def _build_from_scratch(self, input_dfs: BinaryInputs) -> sql.DataFrame:
        return (
            input_dfs["expression_value_df"]
            .groupBy("gene_id")
            .agg(
                F.collect_list(F.struct("case_id", "uqfpkm", "log2_uqfpkm")).alias(
                    "values"
                )
            )
            .select(
                "gene_id",
                F.sort_array("values").alias("values"),
            )
            .select("gene_id", "values.log2_uqfpkm", "values.uqfpkm")
        )
