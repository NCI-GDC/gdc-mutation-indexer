from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from exports import builders, es_utils
from exports.builders import bases, df_builders, utils
from exports.builders.ssm_centric import SSMCentricInputs
from exports.configuration.builders import viz
from exports.constants import build


class SSMCentricInputs(TypedDict):
    case_df: sql.DataFrame
    maf_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class SSMCentricBuilder(bases.IndexBuilder[viz.SSMCentricBuilder, SSMCentricInputs]):
    """
    Builds ssm-centric dataframe given case and maf dataframes::

        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
    """

    __slots__ = ("_consequence_builder", "_observation_builder")

    def __init__(
        self,
        config: viz.SSMCentricBuilder,
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
            input_type=SSMCentricInputs,
            output=build.DataFrame.SSM_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: SSMCentricInputs) -> sql.DataFrame:
        """
        Builds SSM Centric index
        """
        case_df = input_dfs["case_df"]
        maf_df = input_dfs["maf_df"]
        primary_aliquot_df = input_dfs["primary_aliquot_df"]
        ssm_df = df_builders.get_ssm_df(
            maf_df, self._index_name, unique_fields=["ssm_id"]
        )

        cons_df = self._build_consequence(maf_df)
        occurrence_df = self._build_occurrence(maf_df, case_df, primary_aliquot_df)

        ssm_centric_df = ssm_df.join(cons_df, on="ssm_id").join(
            occurrence_df, on="ssm_id"
        )
        ssm_centric_df = utils.filter_large_arrays(
            ssm_centric_df, "occurrence", self._config.occurrences_threshold
        )

        return ssm_centric_df

    def _build_consequence(self, maf_df):
        cons_df = self._consequence_builder.build_for_ssm(
            maf_df, self._index_name, join_gene=True, add_gene_aa_change=True
        )

        return cons_df

    def _build_occurrence(
        self,
        maf_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        # Observation
        obs_df = self._observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self._index_name,
        )
        occurrence_df = (
            case_df.join(obs_df, on=["case_id"], how="right")
            .select(
                "ssm_id",
                F.struct(
                    "occurrence_id",
                    F.struct("observation", *case_df.columns).alias("case"),
                ).alias("occurrence"),
            )
            .groupby("ssm_id")
            .agg(F.collect_list("occurrence").alias("occurrence"))
        )

        return occurrence_df
