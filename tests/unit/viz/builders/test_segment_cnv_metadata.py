import dataclasses
import datetime
from collections.abc import Iterable, Mapping
from unittest import mock

import deepdiff
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import es_utils
from mutation_indexer.constants import build, datamodel
from mutation_indexer.viz import builders, configuration
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class Analysis:
    workflow_type: str = "AscatNGS"
    analysis_id: str = "analysis-0"


@dataclasses.dataclass(frozen=True)
class Aliquot:
    aliquot_id: str = "aliquot-0"
    created_datetime: str | None = datetime.datetime.min.isoformat(timespec="microseconds")

    def to_rdd_data(self) -> dict:
        if self.created_datetime:
            return {
                "aliquot_id": self.aliquot_id,
                "created_datetime": self.created_datetime,
            }

        return {"aliquot_id": self.aliquot_id}


@dataclasses.dataclass(frozen=True)
class Analyte:
    aliquots: tuple[Aliquot, ...] = (Aliquot(),)

    def to_rdd_data(self) -> dict:
        return {"aliquots": tuple(aliquot.to_rdd_data() for aliquot in self.aliquots)}


@dataclasses.dataclass(frozen=True)
class Portion:
    analytes: tuple[Analyte, ...] | None = (Analyte(),)

    def to_rdd_data(self) -> dict:
        analytes = (
            None
            if self.analytes is None
            else tuple(analyte.to_rdd_data() for analyte in self.analytes)
        )

        return {"analytes": analytes}


@dataclasses.dataclass(frozen=True)
class Sample:
    sample_id: str = "sample-0"
    sample_type: str = "Primay Tumor"
    portions: tuple[Portion, ...] = (Portion(),)

    def to_rdd_data(self) -> dict:
        return {
            "sample_id": self.sample_id,
            "portions": tuple(portion.to_rdd_data() for portion in self.portions),
        }


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: str = "case-0"
    samples: tuple[Sample, ...] = (Sample(),)

    def to_rdd_data(self) -> dict:
        return {
            "case_id": self.case_id,
            "samples": tuple(sample.to_rdd_data() for sample in self.samples),
        }


@dataclasses.dataclass(frozen=True)
class File:
    file_id: str = "file-1"
    analysis: Analysis = Analysis()
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "AscatNGS"
    cases: tuple[Case, ...] = (Case(),)
    data_type: str = "Allele-specific Copy Number Segment"

    def to_rdd_data(self) -> tuple:
        return (
            self.file_id,
            {
                "cases": tuple(case.to_rdd_data() for case in self.cases),
                "file_id": self.file_id,
            },
        )


@dataclasses.dataclass(frozen=True)
class AscatMetadataTestData:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "AscatNGS"
    analysis_id: str = "analysis-0"


@pytest.fixture(scope="class")
def ascat_metadata_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCATMetadata.FINAL.load()


@pytest.fixture(scope="class")
def file_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNVMetadata.FILE.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNVMetadata.FINAL.load()


class TestSegmentCNVMetadataBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        spark_session: sql.SparkSession,
        create_dataframe: utils.CreateDataFrame,
        ascat_metadata_schema: types.StructType,
        file_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._spark_session = spark_session
        self._create_dataframe = create_dataframe
        self._ascat_metadata_schema = ascat_metadata_schema
        self._file_schema = file_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> configuration.SegmentCNVMetadataBuilder:
        return mock.MagicMock(
            acl=("open",),
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            use_deprecated_query=False,
            spec=configuration.SegmentCNVMetadataBuilder,
        )

    def _arrange_es_dataframe_util(
        self, files: Iterable[File] = (File(),)
    ) -> es_utils.DataFrameUtil:
        util = mock.MagicMock()
        util.read.return_value = self._create_dataframe(files, self._file_schema)

        return util

    def _arrange_input_dataframes(
        self, test_ascat_metadata: tuple[AscatMetadataTestData, ...]
    ) -> Mapping[str, sql.DataFrame]:
        ascat_metadata_df = self._create_dataframe(
            test_ascat_metadata, self._ascat_metadata_schema
        )

        return {"ascat_metadata_df": ascat_metadata_df}

    def _arrange_builder(
        self, segment_cnv_data: tuple[File, ...]
    ) -> builders.SegmentCNVMetadataBuilder:
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=segment_cnv_data)
        builder = builders.SegmentCNVMetadataBuilder(config, mock.MagicMock(), df_util)

        return builder

    def test__build__joins_single_record(self) -> None:
        """Test joining ascat_metadata dataframe and copy number segment dataframe.

        Given an ascat_metadata dataframe with one gene-level copy number file
        When SegmentCNVMetadataBuilder build is called
        Then return a dataframe with one result and correct schema.
        """
        file = File(
            file_id="file-1",
            analysis=Analysis(
                workflow_type=datamodel.WorkflowType.ASCAT_NGS,
                analysis_id="analysis-0",
            ),
            cases=(
                Case(
                    case_id="case-0",
                    samples=(
                        Sample(
                            portions=(
                                Portion(
                                    analytes=(
                                        Analyte(aliquots=(Aliquot(aliquot_id="aliquot-0"),)),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        inputs = self._arrange_input_dataframes(test_ascat_metadata=(AscatMetadataTestData(),))
        builder = self._arrange_builder(segment_cnv_data=(file,))

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert not deepdiff.DeepDiff(result_df.schema, self._final_schema, ignore_order=True)

    def test__build__input_data_transformed(self) -> None:
        """Test the correctness of the output segment cnv metadata dataframe.

        Given an ascat_metadata dataframe with one gene-level copy number file
        When SegmentCNVMetadataBuilder build is called
        Then return a dataframe with the correct data returned.
        """
        file = File(
            file_id="file-1",
            analysis=Analysis(
                workflow_type=datamodel.WorkflowType.ASCAT_NGS,
                analysis_id="analysis-0",
            ),
            cases=(
                Case(
                    case_id="case-0",
                    samples=(
                        Sample(
                            portions=(
                                Portion(
                                    analytes=(
                                        Analyte(aliquots=(Aliquot(aliquot_id="aliquot-0"),)),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        inputs = self._arrange_input_dataframes(test_ascat_metadata=(AscatMetadataTestData(),))
        builder = self._arrange_builder(segment_cnv_data=(file,))

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.aliquot_id == "aliquot-0"
        assert result_row.case_id == "case-0"
        assert result_row.file_id == "file-1"
        assert result_row.workflow_type == datamodel.WorkflowType.ASCAT_NGS
        assert result_row.analysis_id == "analysis-0"

    def test__build__failed_join(self) -> None:
        """Test that joins do not occur if criteria is not met.

        Given an ascat_metadata dataframe with one gene-level copy number file
            and a copy numer segment file with a differing analysis_id
        When SegmentCNVMetadataBuilder build is called
        Then return a dataframe with no results and correct schema.
        """
        file = File(
            file_id="file-1",
            analysis=Analysis(
                workflow_type=datamodel.WorkflowType.ASCAT_NGS,
                analysis_id="analysis-9999",
            ),
            cases=(
                Case(
                    case_id="case-0",
                    samples=(
                        Sample(
                            portions=(
                                Portion(
                                    analytes=(
                                        Analyte(aliquots=(Aliquot(aliquot_id="aliquot-0"),)),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        inputs = self._arrange_input_dataframes(test_ascat_metadata=(AscatMetadataTestData(),))
        builder = self._arrange_builder(segment_cnv_data=(file,))

        result_df = builder.build(**inputs)

        assert result_df.count() == 0
        assert not deepdiff.DeepDiff(result_df.schema, self._final_schema, ignore_order=True)

    def test__build__filter_non_matching_analysis_ids(self) -> None:
        """Test that non-matching files are filtered out.

        Given an ascat_metadata dataframe with two gene-level copy number file
            and a copy numer segment file that matches analysis_id for only
            one gene-level copy number file
        When SegmentCNVMetadataBuilder build is called
        Then return a dataframe with one result and correct data.
        """
        file = File(
            file_id="file-1",
            analysis=Analysis(
                workflow_type=datamodel.WorkflowType.ASCAT_NGS,
                analysis_id="analysis-0",
            ),
            cases=(
                Case(
                    case_id="case-0",
                    samples=(
                        Sample(
                            portions=(
                                Portion(
                                    analytes=(
                                        Analyte(aliquots=(Aliquot(aliquot_id="aliquot-0"),)),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        inputs = self._arrange_input_dataframes(
            test_ascat_metadata=(
                AscatMetadataTestData(),
                AscatMetadataTestData(
                    aliquot_id="aliquot-1",
                    case_id="case-1",
                    file_id="file-1",
                    workflow_type="ASCAT3",
                    analysis_id="analysis-1",
                ),
            )
        )
        builder = self._arrange_builder(segment_cnv_data=(file,))

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.aliquot_id == "aliquot-0"
        assert result_row.case_id == "case-0"
        assert result_row.file_id == "file-1"
        assert result_row.workflow_type == datamodel.WorkflowType.ASCAT_NGS
        assert result_row.analysis_id == "analysis-0"
