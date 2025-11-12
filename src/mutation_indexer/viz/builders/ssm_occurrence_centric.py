from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import consequence, df_builders, observation


class Inputs(TypedDict):
    maf_df: sql.DataFrame
    case_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class SSMOccurrenceCentricBuilder(
    builders.IndexBuilder[configuration.SSMOccurrenceCentricBuilder, Inputs]
):
    """
    Builds ssm-occurrence-centric dataframe given case and maf dataframes::

        ssm_occurrence{}
              |____ ssm{}
              |        |____ consequence[]
              |                     |_____ transcript{}
              |                                   |_____ gene{}
              |                                   |_____ annotation{}
              |____ case{}
                       |____ observation[]
    """

    def __init__(
        self,
        config: configuration.SSMOccurrenceCentricBuilder,
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
            output=build.DataFrame.SSM_OCCURRENCE_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds SSM Occurrence Centric index
        """
        maf_df = input_dfs["maf_df"]
        case_obs_df = self._build_case_subtree(
            maf_df, input_dfs["case_df"], input_dfs["primary_aliquot_df"]
        )
        ssm_cons = self._build_ssm_subtree(maf_df)

        return (
            ssm_cons.join(case_obs_df, on=["case_id", "ssm_id"], how="inner")
            .withColumn("ssm_occurrence_id", F.col("occurrence_id"))
            .drop("case_id")
            .drop("ssm_id")
            .drop("occurrence_id")
        )

    def _build_ssm_subtree(self, maf_df):
        cons_df = self._consequence_builder.build_for_ssm(
            maf_df,
            self._index_name,
            join_gene=True,
        )
        ssm_df = df_builders.build_ssm_subtree(maf_df, cons_df, self._index_name).drop(
            "gene_id"
        )

        return ssm_df.select(
            "ssm_id",
            "case_id",
            F.struct("consequence", *ssm_df.drop("consequence").drop("case_id").columns).alias(
                "ssm"
            ),
        )

    def _build_case_subtree(
        self,
        maf_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        obs_df = self._observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self._index_name,
        )

        return case_df.join(obs_df, on=["case_id"], how="right").select(
            "case_id",
            "ssm_id",
            "occurrence_id",
            F.struct("observation", *case_df.columns).alias("case"),
        )
