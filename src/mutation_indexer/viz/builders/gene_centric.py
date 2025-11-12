from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import consequence, df_builders, observation


class Inputs(TypedDict):
    ascat_df: sql.DataFrame
    case_df: sql.DataFrame
    maf_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class GeneCentricBuilder(builders.IndexBuilder[configuration.GeneCentricBuilder, Inputs]):
    """
    Builds gene-centric dataframe given case and maf dataframes::

        gene{}
             |___ case[]
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

    def __init__(
        self,
        config: configuration.GeneCentricBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=Inputs,
            output=build.DataFrame.GENE_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds Gene Centric index
        """
        ascat_df = input_dfs["ascat_df"]
        case_df = input_dfs["case_df"]
        maf_df = input_dfs["maf_df"]
        primary_aliquot_df = input_dfs["primary_aliquot_df"]

        gene_df = (
            df_builders.get_gene_df(maf_df, self._index_name, unique_fields=["gene_id"])
            .union(
                df_builders.get_gene_df(ascat_df, self._index_name, unique_fields=["gene_id"])
            )
            .distinct()
        )
        case_df = self._build_case_subtree(maf_df, ascat_df, case_df, primary_aliquot_df)

        return gene_df.join(case_df, on=["gene_id"], how="inner")

    def _build_case_subtree(
        self,
        maf_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """
        case_and_gene_df = self._build_case_with_gene_id(
            maf_df,
            ascat_df,
            case_df,
        )
        ssm_df = self._build_ssm_subtree(maf_df, primary_aliquot_df)
        cnv_df = self._build_cnv_subtree(ascat_df)
        case_subtree = (
            case_and_gene_df.join(ssm_df, on=["gene_id", "case_id"], how="left")
            .join(cnv_df, on=["gene_id", "case_id"], how="left")
            .select(
                "gene_id",
                F.struct("ssm", "cnv", *case_df.drop("gene_id").columns).alias("case"),
            )
        )

        return case_subtree.groupBy(case_subtree.gene_id.alias("gene_id")).agg(
            F.collect_list("case").alias("case")
        )

    def _build_ssm_subtree(
        self, maf_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        TODO: This branch is same as in case_centric and can be reused
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """
        cons_df = self._consequence_builder.build_for_ssm(
            maf_df,
            self._index_name,
        )
        obs_df = self._observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self._index_name,
            selector="ssm",
        )
        obs_df = obs_df.drop("occurrence_id")
        ssm_df = df_builders.build_ssm_subtree(
            maf_df, cons_df, self._index_name, obs_df=obs_df
        )
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
        TODO: This branch is same as in case_centric and can be reused
        cnv[]
           |___ observation[]

        """
        obs_df = self._observation_builder.build_for_cnv(
            ascat_df,
            self._index_name,
            selector="cnv",
        )
        cnv_df = df_builders.build_cnv_subtree(ascat_df, self._index_name, obs_df=obs_df)

        return (
            cnv_df.select(
                "gene_id",
                "case_id",
                F.struct(*cnv_df.drop("gene_id").drop("case_id").columns).alias("cnv"),
            )
            .groupBy(["gene_id", "case_id"])
            .agg(F.collect_list("cnv").alias("cnv"))
        )

    def _build_case_with_gene_id(self, maf_df, ascat_df, case_df):
        maf_and_ascat_df = (
            maf_df.select("case_id", "gene_id")
            .union(ascat_df.select("case_id", "gene_id"))
            .distinct()
        )

        return maf_and_ascat_df.join(case_df, on="case_id").select("gene_id", *case_df.columns)
