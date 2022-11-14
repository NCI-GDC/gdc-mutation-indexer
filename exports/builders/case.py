import abc
import logging

from pyspark import sql
from pyspark.sql import functions as F

import config
from exports import es_utils
from exports.builders import base_input_builder
from exports.constants import build

logging.basicConfig(format=config.LOG_FORMAT)


AVAILABLE_VARIATION_DATA = "available_variation_data"


def _load_available_variation_data(
    maf_metadata_df: sql.DataFrame, ascat_df: sql.DataFrame
) -> sql.DataFrame:
    ssm_data_df = maf_metadata_df.select(
        "case_id", F.lit("ssm").alias(AVAILABLE_VARIATION_DATA)
    )
    cnv_data_df = ascat_df.select("case_id", AVAILABLE_VARIATION_DATA)
    available_variation_df = ssm_data_df.union(cnv_data_df)

    # Finally, group by case
    return available_variation_df.groupby("case_id").agg(
        F.collect_set(AVAILABLE_VARIATION_DATA).alias(AVAILABLE_VARIATION_DATA)
    )


class CaseLoaderMixin(abc.ABC):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    @abc.abstractmethod
    def _load_es_case_data(self) -> sql.DataFrame:
        pass

    def _load_cases(
        self,
        maf_metadata_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        repartition_size: int,
    ) -> sql.DataFrame:
        """
        Builds Case dataframe
        """
        case_df = self._load_es_case_data()
        available_variation_df = _load_available_variation_data(
            maf_metadata_df, ascat_df
        )

        case_df = case_df.join(available_variation_df, on=["case_id"], how="left")

        return case_df.repartition(repartition_size, "case_id")


class CaseBuilder(base_input_builder.BaseInputBuilder, CaseLoaderMixin):
    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        field_selector: es_utils.CaseFieldSelector,
    ) -> None:
        super().__init__(config, sqlContext, "case")

        self._es_dataframe_util = es_dataframe_util
        self._field_selector = field_selector

    def _load_es_case_data(self) -> sql.DataFrame:
        if self.config.projects:  # type: ignore
            query = {"query": {"terms": {"project.project_id": self.config.projects}}}  # type: ignore
        else:
            query = {"query": {"match_all": {}}}

        fields = self._field_selector.select_for(
            build.IndexType.CASE,
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        )

        # Only retrieve the fields we want
        self.logger.info(f"Included fields: {fields}")

        # Load cases from graph index
        return self._es_dataframe_util.get_dataframe(
            build.IndexType.CASE,
            include_fields=fields,
            include_as_arrays=self.config.case_include_as_arrays,
            query=query,
        )

    def build_from_scratch(
        self,
        maf_metadata_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        **kwargs: sql.DataFrame,
    ) -> sql.DataFrame:
        return self._load_cases(
            maf_metadata_df,
            ascat_df,
            self.config.df_repartition,  # type: ignore
        )
