from typing import NamedTuple

import elasticsearch
from pyspark import sql

from mutation_indexer import builders, driver, es_utils, indexd_utils
from mutation_indexer.builders.bases import Builder
from mutation_indexer.configuration import Configuration


class Dependencies(NamedTuple):
    spark_session: sql.SparkSession
    es_client: elasticsearch.Elasticsearch
    es_dataframe_util: es_utils.DataFrameUtil
    es_rdd_util: es_utils.RDDUtil
    doc_dataframe_util: indexd_utils.DataFrameUtil
    case_field_selector: es_utils.CaseFieldSelector
    consequence_builder: builders.ConsequenceBuilder
    observation_builder: builders.ObservationBuilder


class Driver(driver.Driver):
    def _load_builders(self, config: Configuration) -> driver.Iterator[Builder]:
        ge_config = config.builders.gene_expression

        with driver.load_es_client(
            config.elasticsearch.connection
        ) as es_client, driver.load_spark_session() as spark_session:
            mappings_loader = es_utils.MappingsLoader()
            schema_loader = es_utils.SchemaLoader()
            es_dataframe_util = es_utils.DataFrameUtil(
                config.elasticsearch,
                spark_session,
                es_client,
                mappings_loader,
                schema_loader,
            )
            doc_dataframe_util = indexd_utils.DataFrameUtil(
                driver.load_index_client(config.indexd), spark_session
            )

            yield builders.GeneModelBuilder(ge_config.gene_model, spark_session)
            yield builders.GeneExpressionPrimaryAliquotBuilder(
                ge_config.primary_aliquot, spark_session, es_dataframe_util
            )
            yield builders.GeneExpressionIndexBuilder(
                ge_config.gene_expression,
                spark_session,
                es_dataframe_util,
                mappings_loader,
                doc_dataframe_util,
            )


if __name__ == "__main__":
    Driver().run()
