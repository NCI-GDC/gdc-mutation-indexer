import dataclasses
import datetime
import unittest
from typing import Tuple
from unittest import mock

import more_itertools
from pyspark import sql

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import maf_metadata
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import utils
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


class TestMAFMetadataBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._file_schema = schemas.Viz.Builders.MAFMetadata.FILE.load()
        cls._final_schema = schemas.Viz.Builders.MAFMetadata.FINAL.load()

    def arrange_filter_factory(self) -> maf_metadata.MAFFileFilterFactory:
        filter_builder = mock.MagicMock(spec=maf_metadata.MAFFileFilterFactory)

        filter_builder.get_filters.return_value = []

        return filter_builder

    def arrange_builder(
        self, files: Tuple[ESFile, ...] = (ESFile(),)
    ) -> builders.MAFMetadataBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")
        conf = mock.MagicMock(
            spec=viz.MAFMetadataBuilder,
            backup=backup,
            is_cached=False,
            prioritized_experimental_strategies=(),
            acl=(),
        )
        sql_context = mock.MagicMock(spec=sql.SQLContext)
        es_dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        filter_factory = self.arrange_filter_factory()

        conf.projects = None

        es_dataframe_util.read.return_value = utils.create_dataframe(
            files, self._file_schema
        )

        return builders.MAFMetadataBuilder(
            conf, sql_context, es_dataframe_util, filter_factory
        )

    def test__build__single_row(self) -> None:
        builder = self.arrange_builder()

        result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

    def test__build__input_data_transformed(self) -> None:
        file = ESFile()
        builder = self.arrange_builder((file,))

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.case_id == file.cases[0].case_id
        assert result_row.data_type == file.data_type
        assert result_row.file_id == file.file_id
        assert result_row.workflow_type == file.analysis.workflow_type
