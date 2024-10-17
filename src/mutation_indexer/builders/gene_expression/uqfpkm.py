from typing import TypedDict

import numpy
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import aws
from mutation_indexer.builders import bases
from mutation_indexer.configuration.builders import gene_expression
from mutation_indexer.constants import build
from mutation_indexer.constants.build import DataFrame


class ValueArrayInputs(TypedDict):
    expression_value_df: sql.DataFrame


class ValueArrayBuilder(
    bases.InputBuilder[gene_expression.ValueArrayBuilder, ValueArrayInputs]
):
    __slots__ = ("_s3_util", "_value_column")

    def __init__(
        self,
        config: gene_expression.ValueArrayBuilder,
        spark_session: sql.SparkSession,
        s3_util: aws.S3Util,
        output: DataFrame,
        value_column: str,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=ValueArrayInputs, output=output
        )

        self._s3_util = s3_util
        self._value_column = value_column

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        df = super()._write(df)

        for gene_id, values in df.toLocalIterator():
            key = self._config.key_pattern.format(gene_id=gene_id)
            array = numpy.array(values, dtype=numpy.float32)

            self._s3_util.write(self._config.bucket, key, array.tobytes())

        return df

    def _build_from_scratch(self, input_dfs: ValueArrayInputs) -> sql.DataFrame:
        return (
            input_dfs["expression_value_df"]
            .groupBy("gene_id")
            .agg(
                F.collect_list(F.struct("case_id", self._value_column)).alias("values")
            )
            .select(
                "gene_id",
                F.transform(
                    F.sort_array("values"), lambda v: v[self._value_column]
                ).alias("values"),
            )
        )


class UQFPKMBuilder(ValueArrayBuilder):
    def __init__(
        self,
        config: gene_expression.ValueArrayBuilder,
        spark_session: sql.SparkSession,
        s3_util: aws.S3Util,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            s3_util,
            output=build.DataFrame.UQFPKM,
            value_column="uqfpkm",
        )
