from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer.builders import bases
from mutation_indexer.configuration import builders
from mutation_indexer.constants import build


def _rename_columns(gene_model_df: sql.DataFrame) -> sql.DataFrame:
    # Rename transcripts.id to transcripts.transcript_id
    assert isinstance(gene_model_df.schema["transcripts"].dataType, types.ArrayType)
    assert isinstance(
        gene_model_df.schema["transcripts"].dataType.elementType, types.StructType
    )
    tr_schema = gene_model_df.schema["transcripts"].dataType.elementType

    fields = []
    for field in tr_schema.fields:
        if field.name == "id":
            field = types.StructField("transcript_id", types.StringType(), True)
        fields.append(field)

    new_schema = types.ArrayType(types.StructType(fields))  # type: ignore
    gene_df = gene_model_df.select(
        F.col("transcripts").cast(new_schema),
        *gene_model_df.drop("transcripts").columns,
    )

    return gene_df


class GeneModelInputs(TypedDict):
    pass


class GeneModelBuilder(bases.InputBuilder[builders.GeneModelBuilder, GeneModelInputs]):
    """
    Constructs a Gene Model dataframe from ICGC's gene model json
    """

    def __init__(self, config: builders.GeneModelBuilder, spark_session: sql.SparkSession):
        super().__init__(
            config,
            spark_session,
            input_type=GeneModelInputs,
            output=build.DataFrame.GENE_MODEL,
        )

    def _build_from_scratch(self, input_dfs: GeneModelInputs) -> sql.DataFrame:
        """
        Builds Gene Model dataframe
        """
        gene_df, cytobands_df, census_df = self.read_gene_model_files()

        # Join gene model with cytobands data:
        gene_df = gene_df.join(
            cytobands_df, gene_df._gene_id == cytobands_df.ens_gene_id, "left"
        )

        # Join the result with cancer gene census data:
        gene_df = gene_df.join(census_df, gene_df._gene_id == census_df.cancer_gene_id, "left")

        # Drop unnecessary columns
        gene_df = gene_df.drop("ens_gene_id")
        gene_df = gene_df.drop("cancer_gene_id")

        # Rename 'strand' to 'gene_strand'
        gene_df = gene_df.withColumnRenamed("strand", "gene_strand")
        # Rename 'start' to 'gene_start'
        gene_df = gene_df.withColumnRenamed("start", "gene_start")
        # Rename 'end' to 'gene_end'
        gene_df = gene_df.withColumnRenamed("end", "gene_end")

        # Elasticsearch 6+ is strict about how booleans are represented.
        # This column really needs to be lowercase.
        gene_df = gene_df.withColumn(
            "is_cancer_gene_census", F.col("is_cancer_gene_census").cast(types.BooleanType())
        )

        gene_df = _rename_columns(gene_df)

        return gene_df

    def read_gene_model_files(self):
        """
        Reads the gene model, cytobands and census files into Spark
        dataframes
        """
        cytobands_df = self._spark_session.read.csv(
            self._config.citobands_file, sep="\t", header=True
        )

        # Turn the cytoband column into an array of cytobands
        cytobands_df = cytobands_df.withColumn(
            "cytoband",
            F.when(
                F.col("cytoband").isNull() | (F.col("cytoband") == F.lit("")),
                F.array("cytoband"),
            ).otherwise(F.split("cytoband", ",")),
        )

        census_df = self._spark_session.read.csv(
            self._config.census_file, sep="\t", header=True
        )

        gene_model_df = self._spark_session.read.json(self._config.gene_model_file)

        # Flatten, the mapping will re-introduce the structure
        gene_model_df = gene_model_df.select(
            F.col("external_db_ids.*"),
            *gene_model_df.drop("external_db_ids").columns,
        )

        return gene_model_df, cytobands_df, census_df
