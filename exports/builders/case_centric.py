from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import Self

from exports import builders, es_utils, schemas
from exports.builders import case, df_builders
from exports.configuration import adapter
from exports.constants import build


class CaseCentricBuilder(builders.BaseBuilder, case.CaseLoaderMixin):
    """
    Builds case-centric dataframe given case and maf dataframes::

        case{}
             |___ gene[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
    """

    index_name = "case_centric"
    id_field = "case_id"

    def __init__(
        self,
        config: adapter.ObsoleteConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
        field_selector: es_utils.CaseFieldSelector,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ):
        super().__init__(config, sqlContext)

        self._es_dataframe_util = es_dataframe_util
        self._es_rdd_util = es_rdd_util
        self._field_selector = field_selector

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def _load_es_case_data(self) -> sql.DataFrame:
        if (
            False and self.config.projects
        ):  # TODO: Restore func w/ new config specific projects
            query = {"query": {"terms": {"project.project_id": self.config.projects}}}
        else:
            query = {"query": {"match_all": {}}}

        fields = self._field_selector.select_for(
            build.IndexType.CASE,
            build.IndexType.CASE_CENTRIC,
            excluded_fields=("samples",),
        )
        sample_fields = self._field_selector.select_for(
            build.IndexType.CASE,
            build.IndexType.CASE_CENTRIC,
            included_fields=("samples",),
        )

        self.logger.info(f"Included case fields: {fields}")
        self.logger.info(f"Included sample fields: {sample_fields}")

        case_df = self._es_dataframe_util.read(
            build.IndexType.CASE,
            include_fields=fields,
            include_as_arrays=self.config.case_include_as_arrays,
            query=query,
        )
        sample_df = (
            self._es_rdd_util.get_rdd(
                build.IndexType.CASE, include_fields=sample_fields, query=query,
            )
            .toDF(schema=schemas.load_schema("builders/case_centric/sample.yaml"))
            .select("_source.*")
        )

        return case_df.join(sample_df, on="case_id", how="left")

    def build(
        self,
        maf_metadata_df: sql.DataFrame,
        maf_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        **kwargs: sql.DataFrame,
    ) -> Self:
        """
        Builds Case Centric index
        """
        self.log("Building CaseCentric")
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "load":
            self.case_centric = self.load_raw()
            if self.case_centric is not None:
                return self

        case_df = self._load_cases(
            maf_metadata_df, ascat_df, self.config.df_repartition
        )

        self.log("Building Gene subtree")
        gene_subtree = self.build_gene_subtree(maf_df, ascat_df, primary_aliquot_df)

        self.log("Join Case with Gene subtree [left, case_id]")
        case_centric = case_df.join(gene_subtree, on=["case_id"], how="left")
        self.log_count(case_centric)

        self.log("Finalizing case_centric build")
        case_centric = self._final_transform(case_centric)
        self.log_count(case_centric)

        self.case_centric = case_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_gene_subtree(
        self,
        maf_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """

        # TODO Refactor with gene centric.
        self.log("Building Gene from MAF and ASCAT")
        gene_df = df_builders.get_gene_df(
            maf_df,
            self.index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )

        ascat_gene_df = df_builders.get_gene_df(
            ascat_df,
            self.index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )

        gene_df = gene_df.union(ascat_gene_df).distinct()
        self.log_count(gene_df)

        self.log("Building SSM subtree")
        ssm_df = self.build_ssm_subtree(maf_df, primary_aliquot_df)
        self.log_count(ssm_df)

        self.log("Building CNV subtree")
        cnv_df = self.build_cnv_subtree(ascat_df)
        self.log_count(cnv_df)

        self.log("Join SSM and CNV subtrees to Gene [left, gene_id, case_id]")
        gene_ssm_cnv_df = gene_df.join(
            ssm_df, on=["gene_id", "case_id"], how="left"
        ).join(cnv_df, on=["gene_id", "case_id"], how="left")
        self.log_count(gene_ssm_cnv_df)

        self.log("Grouping SSM and CNV subtrees under Gene")
        gene_ssm_cnv_df = gene_ssm_cnv_df.select(
            "case_id",
            F.struct("ssm", "cnv", *gene_df.drop("case_id").columns).alias("gene"),
        )
        self.log_count(gene_df)

        self.log('Grouping by case_id and aggregating to list under "gene"')
        gene_ssm_cnv_df = gene_ssm_cnv_df.groupBy(gene_ssm_cnv_df.case_id).agg(
            F.collect_list("gene").alias("gene")
        )
        return gene_ssm_cnv_df

    def build_ssm_subtree(
        self, maf_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """
        # Consequence
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df, self.index_name, join_gene=False
        )

        # Observation
        obs_df = self.observation_builder.build_for_ssm(
            maf_df, primary_aliquot_df, self.index_name, selector="ssm",
        )
        obs_df = obs_df.drop("occurrence_id")

        # SSM
        ssm_df = df_builders.build_ssm_subtree(
            maf_df, cons_df, self.index_name, obs_df=obs_df
        )

        # Aggregate SSM
        self.log("Aggregating ssm by case_id and gene_id")
        ssm_df = (
            ssm_df.select(
                "gene_id",
                "case_id",
                F.struct(*ssm_df.drop("gene_id").drop("case_id").columns).alias("ssm"),
            )
            .groupBy(["gene_id", "case_id"])
            .agg(F.collect_list("ssm").alias("ssm"))
        )

        return ssm_df

    def build_cnv_subtree(self, ascat_df):
        """
        cnv[]
           |___ observation[]

        """

        # Observation
        obs_df = self.observation_builder.build_for_cnv(
            ascat_df, self.index_name, selector="cnv",
        )

        # Build the final cnv dataframe
        cnv_df = df_builders.build_cnv_subtree(ascat_df, self.index_name, obs_df=obs_df)

        # Aggregate CNV
        self.log("Aggregating cnv by case_id and gene_id")
        cnv_df = (
            cnv_df.select(
                "gene_id",
                "case_id",
                F.struct(*cnv_df.drop("gene_id").drop("case_id").columns).alias("cnv"),
            )
            .groupBy(["gene_id", "case_id"])
            .agg(F.collect_list("cnv").alias("cnv"))
        )

        return cnv_df

    def _final_transform(self, case_centric):
        """
        Final case_centric dataframe transformation:

        - add 'available_variation_data'
        - truncate outliers
        """
        # Coerce any cases that didn't have variation data from None to []
        case_centric = case_centric.withColumn(
            "available_variation_data",
            F.udf(
                lambda x: [] if (x is None) else x, types.ArrayType(types.StringType())
            )(F.col("available_variation_data")),
        )

        # Truncate outliers
        threshold = self.config.percentile_threshold["genes_per_case"]
        case_centric = self.truncate_df_at_percentile(case_centric, "gene", threshold)

        return case_centric
