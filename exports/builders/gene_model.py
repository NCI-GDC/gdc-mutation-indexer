import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import udf, lit, col, regexp_extract
from pyspark.sql.types import *

class GeneModelBuilder(object):
    """
    Constructs a Gene Model dataframe from ICGC's gene model json
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def build(self):
        """
        """
        gene_df, cytobands_df, census_df = self.load()

        # Join gene model with cytobands data:
        gene_df = gene_df.join(cytobands_df,
                               gene_df._gene_id == cytobands_df.ens_gene_id,
                               'left')

        # Join the result with cancer gene census data:
        gene_df = gene_df.join(census_df,
                               gene_df._gene_id == census_df.cancer_gene_id,
                               'left')

        # Drop unnecessary columns
        gene_df = gene_df.drop('ens_gene_id')
        gene_df = gene_df.drop('cancer_gene_id')

        # Rename 'strand' to 'gene_strand'
        gene_df = gene_df.withColumnRenamed('strand', 'gene_strand')

        return gene_df

    def load(self):
        """
        Loads the gene model json file
        """
        spark_csv_path = "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"

        cytobands_df = self.sqlContext\
                           .read\
                           .format(spark_csv_path)\
                           .option("delimiter", "\t") \
                           .option("header", "true")\
                           .load(self.config.citobands_file)

        census_df = self.sqlContext\
                         .read\
                         .format(spark_csv_path)\
                         .option("delimiter", "\t") \
                         .option("header", "true")\
                         .load(self.config.census_file)

        gene_model_df = self.sqlContext.read.json(self.config.gene_model_file)

        # Flatten, the mapping will re-introduce the structure
        gene_model_df = gene_model_df.select(col('external_db_ids.*'),
                                             *gene_model_df.drop('external_db_ids').columns)

        return gene_model_df, cytobands_df, census_df
