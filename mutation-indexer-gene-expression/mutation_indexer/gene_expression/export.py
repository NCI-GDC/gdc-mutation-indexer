import logging

import pyspark
from pyspark import sql

from mutation_indexer.core import configuration
from mutation_indexer.core.constants import logging as logging_constants
from mutation_indexer.driver import es_utils, indexd_utils
from mutation_indexer.gene_expression import builders

logging.basicConfig(format=logging_constants.LOG_FORMAT)

logger = logging.getLogger("mutation_indexer")


class GeneExpressionExporter:
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(
        self,
        sc: pyspark.SparkContext,
        sqlContext: sql.SQLContext,
        config: configuration.ConfigAdapter,
    ) -> None:
        self.config = config
        self.logger = logger
        self.sc = sc
        self.sqlContext = sqlContext

    def run_gene_expression_export(self) -> None:
        """
        Runs the importing, building, and uploading of the gene expression data into elasticsearch.
        """
        es_dataframe_util = es_utils.DataFrameUtil(self.config, self.sqlContext)
        doc_dataframe_util = indexd_utils.DataFrameUtil(
            self.config.indexd, self.sqlContext, logger
        )

        self.sc.setJobGroup("GeneModelBuilder", "Build Gene Model df")
        gene_model_df = builders.GeneModelBuilder(self.config, self.sqlContext).build()

        self.sc.setJobGroup(
            "GeneExpressionPrimaryAliquotBuilder", "Build GE Primary Aliquot df"
        )
        primary_aliquot_df = builders.PrimaryAliquotBuilder(
            self.config, self.sqlContext, es_dataframe_util
        ).build()

        self.sc.setJobGroup("GeneExpressionCaseInputBuilder", "Build GE CaseInput df")
        ge_case_df = builders.CaseBuilder(self.config, self.sqlContext).build(
            gene_expression_primary_aliquot_df=primary_aliquot_df
        )

        self.sc.setJobGroup("GeneExpressionValueInputBuilder", "Build GE ValueInput df")
        ge_values_df = builders.ValueBuilder(
            self.config, self.sqlContext, doc_dataframe_util
        ).build(
            gene_model_df=gene_model_df,
            gene_expression_primary_aliquot_df=primary_aliquot_df,
        )

        self.sc.setJobGroup("gene_expression", "Build {}".format("gene_expression"))
        builders.GeneExpressionBuilder(self.config, self.sqlContext).build(
            ge_case_df, ge_values_df
        ).load()

    def run_export(self) -> None:
        self.run_gene_expression_export()

        self.logger.info("Mutation Indexer finished successfully")
