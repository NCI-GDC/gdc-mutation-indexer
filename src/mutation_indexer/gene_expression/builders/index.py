from typing import TypedDict

from pyspark import sql

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import configuration


class IndexBuilderInputs(TypedDict):
    expression_value_df: sql.DataFrame


class IndexBuilder(builders.IndexBuilder[configuration.IndexBuilder, IndexBuilderInputs]):
    """
    A builder class for loading gene expression data.
    """

    def __init__(
        self,
        config: configuration.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=IndexBuilderInputs,
            output=build.DataFrame.GENE_EXPRESSION,
        )

    def _write_backup(self, df: sql.DataFrame) -> None:
        df.orderBy("gene_id", "case_id").write.parquet(
            self._config.backup.path,
            mode="overwrite",
            partitionBy=self._config.backup.partition_by,
        )

    def _build_from_scratch(self, input_dfs: IndexBuilderInputs) -> sql.DataFrame:
        """
        Creates a data frame with the final gene expression data as found in the
        appropriate data files in indexd. Also adds the calculated log2 value of the
        fpkm_uq_unstranded value.

        Args:
            gene_model_df: The output of the GeneModelBuilder.
            gene_expression_primary_aliquot_df: the output of the
                gene_expression.PrimaryAliquotBuilder

        Returns:
            a data frame of gene expression objects to be loaded into the index.

            gene_expression {}
            |---case_id
            |---gene_expression_id
            |---gene_id
            |---log2_uqfpkm
            |---submitter_id
            |---symbol
            +---uqfpkm
        """
        return input_dfs["expression_value_df"]
