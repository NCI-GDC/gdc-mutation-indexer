import dataclasses
import datetime
from typing import Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders, es_utils
from exports.builders import maf_metadata
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class ESSample:
    sample_id: str = "sample-0"
    sample_type: str = "Primary Tumor"


@dataclasses.dataclass(frozen=True)
class ESCase:
    case_id: str = "case-0"
    samples: Tuple[ESSample, ...] = (ESSample(),)


@dataclasses.dataclass(frozen=True)
class ESAnalysis:
    workflow_type: str = "Aliquot Ensemble Somatic Variant Merging and Masking"


@dataclasses.dataclass(frozen=True)
class ESFile:
    analysis: ESAnalysis = ESAnalysis()
    cases: Tuple[ESCase, ...] = (ESCase(),)
    created_datetime: datetime.datetime = datetime.datetime.min
    data_type: str = "Masked Somatic Mutation"
    file_id: str = "file-0"


@pytest.fixture(scope="class")
def file_schema() -> types.StructType:
    return schemas.Viz.Builders.MAFMetadata.FILE.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.MAFMetadata.FINAL.load()


class TestMAFMetadataBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        file_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.file_schema = file_schema
        self.final_schema = final_schema

    def arrange_filter_factory(self) -> maf_metadata.MAFFileFilterFactory:
        filter_builder = mock.MagicMock(spec=maf_metadata.MAFFileFilterFactory)

        filter_builder.get_filters.return_value = []

        return filter_builder

    def arrange_builder(
        self, files: Tuple[ESFile, ...] = (ESFile(),)
    ) -> builders.MAFMetadataBuilder:
        conf = mock.MagicMock()
        sql_context = mock.MagicMock(spec=sql.SQLContext)
        es_dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        filter_factory = self.arrange_filter_factory()

        conf.projects = None

        es_dataframe_util.get_dataframe.return_value = (
            self.spark_session.createDataFrame(files, schema=self.file_schema)
        )

        return builders.MAFMetadataBuilder(
            conf, sql_context, es_dataframe_util, filter_factory
        )

    def test__build_from_scratch__single_row(self) -> None:
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch()

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build_from_scratch__input_data_transformed(self) -> None:
        file = ESFile()
        builder = self.arrange_builder((file,))

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.case_id == file.cases[0].case_id
        assert result_row.data_type == file.data_type
        assert result_row.file_id == file.file_id
        assert result_row.workflow_type == file.analysis.workflow_type
