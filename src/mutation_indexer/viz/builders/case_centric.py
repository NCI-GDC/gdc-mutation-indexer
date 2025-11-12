import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import case, consequence, df_builders, observation

logger = logging.getLogger(__name__)

SEGMENT_CNV_COLUMNS = (
    "segment_cnv_id",
    "chromosome",
    "length",
    "start_position",
    "end_position",
    "cnv_change",
    "cnv_change_5_category",
)


class Inputs(TypedDict):
    ascat_df: sql.DataFrame
    ascat_metadata_df: sql.DataFrame
    maf_df: sql.DataFrame
    maf_metadata_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame
    segment_cnv_df: sql.DataFrame
    segment_cnv_metadata_df: sql.DataFrame


class CaseCentricBuilder(
    builders.IndexBuilder[configuration.CaseCentricBuilder, Inputs], case.CaseLoaderMixin
):
    """
    Builds case-centric dataframe given case, maf, and segment_cnv dataframes:

        case{}
            |___ gene[]
            |        |___ ssm[]
            |        |     |___ consequence[]
            |        |     |             |_____ transcript{}
            |        |     |                          |_____ annotation{}
            |        |     |___ observation[]
            |        |
            |        |___ cnv[]
            |              |___ consequence[]
            |              |            |_____ gene{}
            |              |
            |              |___ observation[]
            |___ segment_cnv[]
                     |____ observation[]
    """

    def __init__(
        self,
        config: configuration.CaseCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        field_selector: es_utils.CaseFieldSelector,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=Inputs,
            output=build.DataFrame.CASE_CENTRIC,
        )

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
        )

        logger.info(f"Included case fields: {fields}")

        return self._es_dataframe_util.read(
            build.IndexType.CASE,
            source_filter=fields,
            include_as_arrays=self._config.include_as_arrays,
            query=query,
        )

    def _build_segment_cnv_subtree(self, segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
        """Aggregates all segment_cnvs for each case.

        segment_cnv_subtree{}
            |____ case_id
            |____ segment_cnv []
                    |____ observation[]

        STEPS:
            1) Select the pertinent segment_cnv columns.

            2) Build the observation dataframe, which will collect all observations for
            each segment_cnv_id and case_id combination.

            3) Join the observation dataframe with a filtered segment_cnv_df. Before the
            join, we want to drop duplicate rows based on segment_cnv_id to reduce
            amount of work.

            4) Aggregate all segment_cnvs from step 3 and create a list of segment_cnvs
            associated with each case. The case_id is required to be able to join back to
            the final case_centric dataframe.
        """
        obs_df = observation.build_observation_for_segment_cnv(segment_cnv_df).select(
            "segment_cnv_id", "case_id", "observation"
        )
        segment_cnv_df = segment_cnv_df.select(*SEGMENT_CNV_COLUMNS).distinct()
        subtree_df = segment_cnv_df.join(obs_df, on="segment_cnv_id", how="inner").select(
            F.struct(*SEGMENT_CNV_COLUMNS, "observation").alias("segment_cnv"),
            "segment_cnv_id",
            "case_id",
        )
        subtree_df = subtree_df.groupBy("case_id").agg(
            F.collect_set("segment_cnv").alias("segment_cnv")
        )
        subtree_df = subtree_df.select("case_id", "segment_cnv")

        return subtree_df

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds Case Centric index
        """
        case_df = self._load_cases(
            input_dfs["maf_metadata_df"],
            input_dfs["ascat_metadata_df"],
            input_dfs["segment_cnv_metadata_df"],
            self._config.partition_size,
        )
        gene_df = self._build_gene_subtree(
            input_dfs["maf_df"], input_dfs["ascat_df"], input_dfs["primary_aliquot_df"]
        )
        segment_cnv_df = self._build_segment_cnv_subtree(input_dfs["segment_cnv_df"])
        case_centric_df = case_df.join(gene_df, on=["case_id"], how="left").join(
            segment_cnv_df, on=["case_id"], how="left"
        )

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

        # TODO Refactor with gene centric.
        gene_df = df_builders.get_gene_df(
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

        gene_df = gene_df.union(ascat_gene_df).distinct()
        ssm_df = self._build_ssm_subtree(maf_df, primary_aliquot_df)
        cnv_df = self._build_cnv_subtree(ascat_df)
        gene_ssm_cnv_df = gene_df.join(ssm_df, on=["gene_id", "case_id"], how="left").join(
            cnv_df, on=["gene_id", "case_id"], how="left"
        )
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

    def _build_cnv_subtree(self, ascat_df):
        """
        cnv[]
           |___ observation[]

        """

        # Observation
        obs_df = self._observation_builder.build_for_cnv(
            ascat_df,
            self._index_name,
            selector="cnv",
        )

        # Build the final cnv dataframe
        cnv_df = df_builders.build_cnv_subtree(ascat_df, self._index_name, obs_df=obs_df)

        # Aggregate CNV
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
            F.udf(lambda x: [] if (x is None) else x, types.ArrayType(types.StringType()))(
                F.col("available_variation_data")
            ),
        )

        # Truncate outliers
        case_centric = utils.filter_arrays_by_relative_size(
            case_centric, "gene", self._config.genes_threshold
        )

        return case_centric
