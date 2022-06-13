from pyspark import sql

from mutation_indexer import driver
from mutation_indexer.gene_expression import export


def main() -> None:
    """
    Define the spark context and parse agruments into config
    """
    config = driver.get_configuration()
    config_adapter = driver.get_conifg_adapter(config)

    with driver.initialize_spark(config.spark_arguments) as spark_session:
        sql_context = sql.SQLContext(spark_session.sparkContext)
        exporter = export.GeneExpressionExporter(
            spark_session.sparkContext, sql_context, config_adapter
        )

        exporter.run_export()
