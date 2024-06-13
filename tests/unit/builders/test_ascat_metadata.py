import dataclasses
import datetime
from collections.abc import Iterable
from typing import Optional
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders, es_utils
from mutation_indexer.builders import ascat_metadata
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass
class Analysis:
    workflow_type: str = "AscatNGS"


@dataclasses.dataclass(frozen=True)
class Aliquot:
    aliquot_id: str = "a-0"
    created_datetime: Optional[str] = datetime.datetime.min.isoformat(
        timespec="microseconds"
    )

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
    analytes: Optional[tuple[Analyte, ...]] = (Analyte(),)

    def to_rdd_data(self) -> dict:
        analytes = (
            None
            if self.analytes is None
            else tuple(analyte.to_rdd_data() for analyte in self.analytes)
        )

        return {"analytes": analytes}


@dataclasses.dataclass(frozen=True)
class Sample:
    sample_id: str = "s-0"
    sample_type: str = "Primay Tumor"
    portions: tuple[Portion, ...] = (Portion(),)

    def to_rdd_data(self) -> dict:
        return {
            "sample_id": self.sample_id,
            "portions": tuple(portion.to_rdd_data() for portion in self.portions),
        }


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: str = "c-0"
    samples: tuple[Sample, ...] = (Sample(),)

    def to_rdd_data(self) -> dict:
        return {
            "case_id": self.case_id,
            "samples": tuple(sample.to_rdd_data() for sample in self.samples),
        }


@dataclasses.dataclass(frozen=True)
class File:
    file_id: str = "f-0"
    analysis: Analysis = Analysis()
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "WXS"
    cases: tuple[Case, ...] = (Case(),)

    def to_rdd_data(self) -> tuple:
        return (
            self.file_id,
            {
                "cases": tuple(case.to_rdd_data() for case in self.cases),
                "file_id": self.file_id,
            },
        )


@pytest.fixture(scope="class")
def file_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCATMetadata.FILE.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCATMetadata.FINAL.load()


class TestASCATMetadataBuilder:
    @pytest.fixture(autouse=True)
    def init_fixtures(
        self,
        spark_session: sql.SparkSession,
        create_dataframe: utils.CreateDataFrame,
        file_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._spark_session = spark_session
        self._create_dataframe = create_dataframe
        self._file_schema = file_schema
        self._final_schema = final_schema

    def _arrange_config(self) -> viz.Builder:
        return mock.MagicMock(
            acl=("open",),
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            spec=viz.Builder,
        )

    def _arrange_es_dataframe_util(
        self, files: Iterable[File] = (File(),)
    ) -> es_utils.DataFrameUtil:
        util = mock.MagicMock()
        util.read.return_value = self._create_dataframe(files, self._file_schema)

        return util

    def _arrange_es_rdd_util(
        self, files: Iterable[File] = (File(),)
    ) -> es_utils.RDDUtil:
        spark_context = self._spark_session.sparkContext
        util = mock.MagicMock(spec=es_utils.RDDUtil)

        util.get_rdd.return_value = spark_context.parallelize(
            file.to_rdd_data() for file in files
        )

        return util

    def test__build__single_row(self) -> None:
        file = File(
            file_id="file-0",
            analysis=Analysis(workflow_type=ascat_metadata.ABSOLUTE),
            cases=(
                Case(
                    case_id="case-0",
                    samples=(
                        Sample(
                            portions=(
                                Portion(
                                    analytes=(
                                        Analyte(
                                            aliquots=(Aliquot(aliquot_id="aliquot-0"),)
                                        ),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=(file,))
        rdd_util = self._arrange_es_rdd_util(files=(file,))
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self._final_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.aliquot_id == "aliquot-0"
        assert result_row.case_id == "case-0"
        assert result_row.file_id == "file-0"
        assert result_row.workflow_type == ascat_metadata.ABSOLUTE

    @pytest.mark.parametrize(
        ("primay_sample_type", "other_sample_type"),
        (
            ("Primary Tumor", "Primary Blood Derived Cancer - Bone Marrow"),
            (
                "Primary Blood Derived Cancer - Bone Marrow",
                "Primary Blood Derived Cancer - Peripheral Blood",
            ),
            ("Primary Blood Derived Cancer - Peripheral Blood", "Metastatic"),
            ("Metastatic", "Additional Metastatic"),
            ("Additional Metastatic", "Recurrent Tumor"),
            ("Recurrent Tumor", "Recurrent Blood Derived Cancer - Bone Marrow"),
            (
                "Recurrent Blood Derived Cancer - Bone Marrow",
                "Recurrent Blood Derived Cancer - Peripheral Blood",
            ),
            (
                "Recurrent Blood Derived Cancer - Peripheral Blood",
                "Additional - New Primary",
            ),
            ("Additional - New Primary", "OTHER"),
        ),
    )
    def test__build__sample_type_selection(
        self, primay_sample_type: str, other_sample_type: str
    ) -> None:
        other_portions = (
            Portion(analytes=(Analyte(aliquots=(Aliquot(aliquot_id="a-0"),)),)),
        )
        primary_portions = (
            Portion(analytes=(Analyte(aliquots=(Aliquot(aliquot_id="a-1"),)),)),
        )
        samples = (
            Sample(
                sample_id="s-0",
                sample_type=other_sample_type,
                portions=other_portions,
            ),
            Sample(
                sample_id="s-1",
                sample_type=primay_sample_type,
                portions=primary_portions,
            ),
        )
        file = File(cases=(Case(samples=samples),))
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=(file,))
        rdd_util = self._arrange_es_rdd_util(files=(file,))
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.aliquot_id == "a-1"

    @pytest.mark.parametrize(
        ("primary_datetime", "other_datetime"),
        (
            (
                datetime.datetime.max - datetime.timedelta(microseconds=1),
                datetime.datetime.max,
            ),
            (
                datetime.datetime(
                    1970,
                    1,
                    12,
                    8,
                    45,
                    34,
                    203025,
                    datetime.timezone(datetime.timedelta(hours=-5)),
                ),
                datetime.datetime(
                    1970,
                    1,
                    12,
                    8,
                    45,
                    34,
                    203025,
                    datetime.timezone(datetime.timedelta(hours=-6)),
                ),
            ),
        ),
        ids=("microsecond_diff", "timezone_diff"),
    )
    def test__build__file_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        files = (
            File(
                file_id="f-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            File(
                file_id="f-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files)
        rdd_util = self._arrange_es_rdd_util(files)
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == "f-1"

    def test__build__file_id(self) -> None:
        files = (File(file_id="f-1"), File(file_id="f-0"))
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files)
        rdd_util = self._arrange_es_rdd_util(files=())
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == "f-0"

    def test__build__aliquot_none(self) -> None:
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util()
        rdd_util = self._arrange_es_rdd_util(files=())
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id is None for row in result_rows)

    @pytest.mark.parametrize(
        ("primary_datetime", "other_datetime"),
        (
            (
                datetime.datetime.max - datetime.timedelta(microseconds=1),
                datetime.datetime.max,
            ),
            (
                datetime.datetime(
                    1970,
                    1,
                    12,
                    8,
                    45,
                    34,
                    203025,
                    datetime.timezone(datetime.timedelta(hours=-5)),
                ),
                datetime.datetime(
                    1970,
                    1,
                    12,
                    8,
                    45,
                    34,
                    203025,
                    datetime.timezone(datetime.timedelta(hours=-6)),
                ),
            ),
        ),
        ids=("microsecond_diff", "timezone_diff"),
    )
    def test__build__aliquot_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        aliquots = (
            Aliquot(
                aliquot_id="a-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            Aliquot(
                aliquot_id="a-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        sample = Sample(portions=(Portion(analytes=(Analyte(aliquots=aliquots),)),))
        file = File(cases=(Case(samples=(sample,)),))
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=(file,))
        rdd_util = self._arrange_es_rdd_util(files=(file,))
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-1" for row in result_rows)

    def test__build__aliquot_id(self) -> None:
        aliquots = (Aliquot(aliquot_id="a-0"), Aliquot(aliquot_id="a-1"))
        sample = Sample(portions=(Portion(analytes=(Analyte(aliquots=aliquots),)),))
        file = File(cases=(Case(samples=(sample,)),))
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=(file,))
        rdd_util = self._arrange_es_rdd_util(files=(file,))
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-0" for row in result_rows)

    @pytest.mark.parametrize(
        ("unprioritized_workflow", "prioritized_workflow"),
        (
            (ascat_metadata.ASCAT_NGS, ascat_metadata.ASCAT2),
            (ascat_metadata.ASCAT2, ascat_metadata.ASCAT3),
            (ascat_metadata.ASCAT3, ascat_metadata.ABSOLUTE),
        ),
    )
    def test__build__workflow_type(
        self, unprioritized_workflow: str, prioritized_workflow: str
    ) -> None:
        files = (
            File(analysis=Analysis(workflow_type=unprioritized_workflow)),
            File(analysis=Analysis(workflow_type=prioritized_workflow)),
        )
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=files)
        rdd_util = self._arrange_es_rdd_util(files=files)
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()
        result_rows = result_df.collect()

        assert len(result_rows) == 1
        assert result_rows[0].workflow_type == prioritized_workflow

    def test__build__missing_analytes(self) -> None:
        aliquots = (Aliquot(aliquot_id="a-0"), Aliquot(aliquot_id="a-1"))
        sample = Sample(
            portions=(
                Portion(analytes=(Analyte(aliquots=aliquots),)),
                Portion(analytes=None),
            )
        )
        file = File(cases=(Case(samples=(sample,)),))
        config = self._arrange_config()
        df_util = self._arrange_es_dataframe_util(files=(file,))
        rdd_util = self._arrange_es_rdd_util(files=(file,))
        builder = builders.ASCATMetadataBuilder(
            config, mock.MagicMock(), df_util, rdd_util
        )

        result_df = builder.build()

        assert result_df.count() == 1
