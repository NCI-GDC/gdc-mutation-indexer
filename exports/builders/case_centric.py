import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from exports import builders, es_utils, schemas
from exports.builders import bases, case, df_builders, utils
from exports.configuration.builders import viz
from exports.constants import build

logger = logging.getLogger(__name__)


class CaseCentricInputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    maf_df: sql.DataFrame
    ascat_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class CaseCentricBuilder(
    bases.IndexBuilder[viz.CaseCentricBuilder, CaseCentricInputs], case.CaseLoaderMixin
):
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

    __slots__ = (
        "_es_rdd_util",
        "_field_selector",
        "_consequence_builder",
        "_observation_builder",
    )

    def __init__(
        self,
        config: viz.CaseCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        es_rdd_util: es_utils.RDDUtil,
        field_selector: es_utils.CaseFieldSelector,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=CaseCentricInputs,
            output=build.DataFrame.CASE_CENTRIC,
        )

        self._es_rdd_util = es_rdd_util
        self._field_selector = field_selector
        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _load_es_case_data(self) -> sql.DataFrame:
        if self._config.projects:
            query = {"query": {"terms": {"project.project_id": self._config.projects}}}
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

        logger.info(f"Included case fields: {fields}")
        logger.info(f"Included sample fields: {sample_fields}")

        case_df = self._es_dataframe_util.read(
            build.IndexType.CASE,
            include_fields=fields,
            include_as_arrays=self._config.include_as_arrays,
            query=query,
        )
        sample_df = (
            self._es_rdd_util.get_rdd(
                build.IndexType.CASE,
                include_fields=sample_fields,
                query=query,
            )
            .toDF(schema=schemas.load_schema("builders/case_centric/sample.yaml"))
            .select("_source.*")
        )

        return case_df.join(sample_df, on="case_id", how="left")

    def _build_from_scratch(self, input_dfs: CaseCentricInputs) -> sql.DataFrame:
        """
        Builds Case Centric index
        """
        ascat_df = input_dfs["ascat_df"]
        case_df = self._load_cases(input_dfs["maf_metadata_df"], ascat_df, 2048)
        gene_subtree_df = self._build_gene_subtree(
            input_dfs["maf_df"], ascat_df, input_dfs["primary_aliquot_df"]
        )
        case_centric_df = case_df.join(gene_subtree_df, on=["case_id"], how="left")
        case_centric_df = self._final_transform(case_centric_df)

        return case_centric_df

    def _build_gene_subtree(
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
        maf_gene_df = df_builders.get_gene_df(
            maf_df,
            self._index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )
        ascat_gene_df = df_builders.get_gene_df(
            ascat_df,
            self._index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )
        gene_df = maf_gene_df.union(ascat_gene_df).distinct()

        ssm_df = self._build_ssm_subtree(maf_df, primary_aliquot_df)
        cnv_df = self._build_cnv_subtree(ascat_df)

        gene_ssm_cnv_df = gene_df.join(
            ssm_df, on=["gene_id", "case_id"], how="left"
        ).join(cnv_df, on=["gene_id", "case_id"], how="left")
        gene_ssm_cnv_df = gene_ssm_cnv_df.select(
            "case_id",
            F.struct("ssm", "cnv", *gene_df.drop("case_id").columns).alias("gene"),
        )
        gene_ssm_cnv_df = gene_ssm_cnv_df.groupBy(gene_ssm_cnv_df.case_id).agg(
            F.collect_list("gene").alias("gene")
        )

        return gene_ssm_cnv_df

    def _build_ssm_subtree(
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
        cons_df = self._consequence_builder.build_for_ssm(
            maf_df, self._index_name, join_gene=False
        )

        # Observation
        obs_df = self._observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self._index_name,
            selector="ssm",
        )
        obs_df = obs_df.drop("occurrence_id")

        # SSM
        ssm_df = df_builders.build_ssm_subtree(
            maf_df, cons_df, self._index_name, obs_df=obs_df
        )

        # Aggregate SSM
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

    def _build_cnv_subtree(self, ascat_df: sql.DataFrame) -> sql.DataFrame:
        """
        cnv[]
           |___ observation[]

        """
        observation_df = self._observation_builder.build_for_cnv(
            ascat_df,
            self._index_name,
            selector="cnv",
        )
        cnv_df = df_builders.build_cnv_subtree(
            ascat_df, self._index_name, obs_df=observation_df
        )
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

    def _final_transform(self, case_centric_df: sql.DataFrame) -> sql.DataFrame:
        """
        Final case_centric dataframe transformation:

        - add 'available_variation_data'
        - truncate outliers
        """
        # Coerce any cases that didn't have variation data from None to []
        case_centric_df = case_centric_df.withColumn(
            "available_variation_data",
            F.coalesce("available_variation_data", F.array()),
        )
        case_centric_df = utils.filter_large_arrays(
            case_centric_df, "gene", self._config.genes_threshold
        )

        return case_centric_df
