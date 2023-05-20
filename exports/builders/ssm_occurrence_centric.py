from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from exports import builders, es_utils
from exports.builders import bases, df_builders
from exports.configuration.builders import viz
from exports.constants import build


class SSMOccurrenceCentricInputs(TypedDict):
    maf_df: sql.DataFrame
    case_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class SSMOccurrenceCentricBuilder(
    bases.IndexBuilder[viz.IndexBuilder, SSMOccurrenceCentricInputs]
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

    __slots__ = ("_consequence_builder", "_observation_builder")

    def __init__(
        self,
        config: viz.IndexBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            mappings_loader,
            input_type=SSMOccurrenceCentricInputs,
            output=build.DataFrame.SSM_OCCURRENCE_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(
        self, input_dfs: SSMOccurrenceCentricInputs
    ) -> sql.DataFrame:
        """
        Builds SSM Occurrence Centric index
        """
        case_df = input_dfs["case_df"]
        maf_df = input_dfs["maf_df"]
        primary_aliquot_df = input_dfs["primary_aliquot_df"]

        case_observation_df = self._build_case_subtree(
            maf_df, case_df, primary_aliquot_df
        )
        ssm_consequence_df = self._build_ssm_subtree(maf_df)

        ssm_occurrence_centric_df = (
            ssm_consequence_df.join(
                case_observation_df, on=["case_id", "ssm_id"], how="inner"
            )
            .withColumn("ssm_occurrence_id", F.col("occurrence_id"))
            .drop("case_id")
            .drop("ssm_id")
            .drop("occurrence_id")
        )

        return ssm_occurrence_centric_df

    def _build_ssm_subtree(self, maf_df):
        consequence_df = self._consequence_builder.build_for_ssm(
            maf_df,
            self._index_name,
            join_gene=True,
        )
        ssm_df = df_builders.build_ssm_subtree(
            maf_df, consequence_df, self._index_name
        ).drop("gene_id")
        ssm_consequence_df = ssm_df.select(
            "ssm_id",
            "case_id",
            F.struct(
                "consequence", *ssm_df.drop("consequence").drop("case_id").columns
            ).alias("ssm"),
        )

        return ssm_consequence_df

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
        case_obs_df = case_df.join(obs_df, on=["case_id"], how="right").select(
            "case_id",
            "ssm_id",
            "occurrence_id",
            F.struct("*").dropFields("ssm_id", "occurrence_id").alias("case"),
        )

        return case_obs_df
