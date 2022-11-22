import importlib_resources as resources
from pyspark import sql
from pyspark.sql import functions as F

from exports.builders import bases
from exports.configuration.builders import viz
from exports.constants import app, build


class CivicDNABuilder(bases.InputBuilder[viz.CivicDNABuilder]):
    def __init__(
        self, config: viz.CivicDNABuilder, spark_session: sql.SparkSession
    ) -> None:
        super().__init__(config, spark_session, build.DataFrame.CIVIC_DNA)

    def _load_resource(self) -> sql.DataFrame:
        with resources.as_file(
            resources.files(app.MUTATION_INDEXER_RESOURCES)
        ) as resource_dir:
            dna_file = str(resource_dir / self._config.dna_file)
            dna_df = self._spark_session.read.csv(dna_file, sep="\t")

        return dna_df

    def _build_from_scratch(self, **_: sql.DataFrame) -> sql.DataFrame:
        dna_df = self._load_resource()

        dna_df = dna_df.select(
            "chromosome",
            "start_position",
            "reference_allele",
            F.col("alternative_allele").alias("tumor_allele"),
            "civic_gene_id",
            F.col("civic_var_id").alias("civic_variant_id"),
        )

        return dna_df


class CivicProtBuilder(bases.InputBuilder[viz.CivicProtBuilder]):
    def __init__(
        self,
        config: viz.CivicProtBuilder,
        spark_session: sql.SparkSession,
    ) -> None:
        super().__init__(config, spark_session, build.DataFrame.CIVIC_PROT)

    def _load_resource(self) -> sql.DataFrame:
        with resources.as_file(
            resources.files(app.MUTATION_INDEXER_RESOURCES)
        ) as resource_dir:
            prot_file = str(resource_dir / self._config.prot_file)
            prot_df = self._spark_session.read.csv(prot_file, sep="\t")

        return prot_df

    def _build_from_scratch(self, **_: sql.DataFrame) -> sql.DataFrame:
        prot_df = self._load_resource()

        prot_df = prot_df.select(
            F.col("hugo_symbol").alias("name"),
            F.col("hgvsp").alias("hgvsp_short"),
            "civic_gene_id",
            F.col("civic_var_id").alias("civic_variant_id"),
        )

        return prot_df
