from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from mutation_indexer.viz.builders import consequence, df_builders, observation


class Inputs(TypedDict):
    case_df: sql.DataFrame
    maf_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


class SSMCentricBuilder(builders.IndexBuilder[configuration.SSMCentricBuilder, Inputs]):
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

    def __init__(
        self,
        config: configuration.SSMCentricBuilder,
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
            output=build.DataFrame.SSM_CENTRIC,
        )

        self._consequence_builder = consequence_builder
        self._observation_builder = observation_builder

    def _build_from_scratch(self, input_dfs: Inputs) -> sql.DataFrame:
        """
        Builds SSM Centric index
        """
        maf_df = input_dfs["maf_df"]
        ssm_df = df_builders.get_ssm_df(maf_df, self._index_name, unique_fields=["ssm_id"])
        cons_df = self._build_consequence(maf_df)
        occurrence_df = self._build_occurrence(
            maf_df, input_dfs["case_df"], input_dfs["primary_aliquot_df"]
        )
        ssm_centric = ssm_df.join(cons_df, on="ssm_id").join(occurrence_df, on="ssm_id")

        return utils.filter_arrays_by_relative_size(
            ssm_centric, "occurrence", self._config.occurrences_threshold
        )

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
        obs_df = self._observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self._index_name,
        )

        return (
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
