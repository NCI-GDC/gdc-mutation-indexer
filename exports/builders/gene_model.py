import logging

from pyspark.sql.functions import udf, col
from pyspark.sql.types import (
    StringType, ArrayType, StructType, StructField
)

logging.basicConfig()


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
        Builds Gene Model dataframe
        """
        gene_df, cytobands_df, census_df = self.read_gene_model_files()

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
        # Rename 'start' to 'gene_start'
        gene_df = gene_df.withColumnRenamed('start', 'gene_start')
        # Rename 'end' to 'gene_end'
        gene_df = gene_df.withColumnRenamed('end', 'gene_end')

        gene_df = self.rename_columns(gene_df)

        return gene_df

    def read_gene_model_files(self):
        """
        Reads the gene model, cytobands and census files into Spark
        dataframes
        """
        spark_csv_path = "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"

        cytobands_df = self.sqlContext\
                           .read\
                           .format(spark_csv_path)\
                           .option("delimiter", "\t") \
                           .option("header", "true")\
                           .load(self.config.citobands_file)

        # Turn the cytoband column into an array of cytobands
        cytobands_df = cytobands_df.withColumn('cytoband',
                                udf(lambda x: x.split(',') if x else [x],
                                    ArrayType(StringType()))(col('cytoband')))

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

    @staticmethod
    def rename_columns(gene_df):
        # Rename transcripts.id to transcripts.transcript_id
        tr_schema = gene_df.schema['transcripts'].dataType.elementType

        fields = []
        for field in tr_schema.fields:
            if field.name == 'id':
                field = StructField('transcript_id', StringType(), True)
            fields.append(field)

        new_schema = ArrayType(StructType(fields))
        gene_df = gene_df.select(col("transcripts").cast(new_schema),
                                 *gene_df.drop('transcripts').columns)

        return gene_df
