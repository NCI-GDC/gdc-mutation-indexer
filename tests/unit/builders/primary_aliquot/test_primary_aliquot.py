import collections
import dataclasses
import datetime
import unittest
from os import path
from typing import Dict, Iterable, Optional, Tuple
from unittest import mock

import more_itertools
import pytest
import yaml
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from exports import builders, es_utils
from exports.constants import build
from tests.integration.utils import schema_validation
from tests.unit.data.schemas import viz_schemas


@dataclasses.dataclass(frozen=True)
class ESAliquot:
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
class ESAnalyte:
    aliquots: Tuple[ESAliquot, ...] = (ESAliquot(),)

    def to_rdd_data(self) -> dict:
        return {"aliquots": tuple(aliquot.to_rdd_data() for aliquot in self.aliquots)}


@dataclasses.dataclass(frozen=True)
class ESPortion:
    analytes: Optional[Tuple[ESAnalyte, ...]] = (ESAnalyte(),)

    def to_rdd_data(self) -> dict:
        analytes = (
            None
            if self.analytes is None
            else tuple(analyte.to_rdd_data() for analyte in self.analytes)
        )

        return {"analytes": analytes}


@dataclasses.dataclass(frozen=True)
class ESSample:
    sample_id: str = "s-0"
    sample_type: str = "Primay Tumor"
    portions: Tuple[ESPortion, ...] = (ESPortion(),)

    def to_rdd_data(self) -> dict:
        return {
            "sample_id": self.sample_id,
            "portions": tuple(portion.to_rdd_data() for portion in self.portions),
        }


@dataclasses.dataclass(frozen=True)
class ESCase:
    case_id: str = "c-0"
    samples: Tuple[ESSample, ...] = (ESSample(),)

    def to_rdd_data(self) -> dict:
        return {
            "case_id": self.case_id,
            "samples": tuple(sample.to_rdd_data() for sample in self.samples),
        }


@dataclasses.dataclass(frozen=True)
class ESFile:
    file_id: str = "f-0"
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "WXS"
    cases: Tuple[ESCase, ...] = (ESCase(),)

    def to_rdd_data(self) -> tuple:
        return (
            self.file_id,
            {
                "cases": tuple(case.to_rdd_data() for case in self.cases),
                "file_id": self.file_id,
            },
        )


@pytest.fixture(scope="class")
def input_file_schema() -> types.StructType:
    return viz_schemas.PrimaryAliquot.INPUT_ES_FILE.load_schema()


class TestPrimaryAliquotBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self, spark_session: sql.SparkSession, input_file_schema: types.StructType
    ) -> None:
        self.spark_session = spark_session
        self.input_file_schema = input_file_schema

    def _arrange_es_rdd_util(self, files: Iterable[ESFile]) -> es_utils.RDDUtil:
        spark_context = self.spark_session.sparkContext
        rdd_util = mock.MagicMock(spec=es_utils.RDDUtil)

        rdd_util.get_rdd.return_value = spark_context.parallelize(
            file.to_rdd_data() for file in files
        )

        return rdd_util

    def _arrange_es_dataframe_util(
        self,
        files: Iterable[ESFile],
    ) -> es_utils.DataFrameUtil:
        files = files if isinstance(files, tuple) else tuple(files)
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        file_df = self.spark_session.createDataFrame(
            files, schema=self.input_file_schema
        )

        dataframe_util.get_dataframe.return_value = file_df

        return dataframe_util

    def _arrange_builder(
        self,
        es_files: Tuple[ESFile, ...],
        aliquot_data: Optional[Tuple[ESFile, ...]] = None,
    ) -> builders.PrimaryAliquotBuilder:
        aliquot_data = es_files if aliquot_data is None else aliquot_data
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        indexd = mock.MagicMock()
        dataframe_util = self._arrange_es_dataframe_util(es_files)
        rdd_util = self._arrange_es_rdd_util(aliquot_data)

        return builders.PrimaryAliquotBuilder(
            config, sql_context, indexd, dataframe_util, rdd_util
        )

    @pytest.mark.parametrize(
        ("files", "aliquot_data"),
        (((ESFile(),), (ESFile(),)), ((ESFile(),), ())),
        ids=("aliquot_exists", "no_aliquots"),
    )
    def test__build_from_scratch__positive_joins(
        self, files: Iterable[ESFile], aliquot_data: Iterable[ESFile]
    ) -> None:
        builder = self._arrange_builder(files, aliquot_data)

        result_df = builder.build()

        assert result_df.count() == 2

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
    def test__build_from_scratch__sample_type_selection(
        self, primay_sample_type: str, other_sample_type: str
    ) -> None:
        other_portions = (
            ESPortion(analytes=(ESAnalyte(aliquots=(ESAliquot(aliquot_id="a-0"),)),)),
        )
        primary_portions = (
            ESPortion(analytes=(ESAnalyte(aliquots=(ESAliquot(aliquot_id="a-1"),)),)),
        )
        samples = (
            ESSample(
                sample_id="s-0",
                sample_type=other_sample_type,
                portions=other_portions,
            ),
            ESSample(
                sample_id="s-1",
                sample_type=primay_sample_type,
                portions=primary_portions,
            ),
        )
        file = ESFile(cases=(ESCase(samples=samples),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()
        result_case_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )
        result_file_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("file")).collect()
        )

        assert result_case_row.aliquot_id == "a-1"
        assert result_file_row.aliquot_id == "a-1"

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
    def test__build_from_scratch__file_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        files = (
            ESFile(
                file_id="f-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            ESFile(
                file_id="f-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        builder = self._arrange_builder(files, ())

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-1"

    def test__build_from_scratch__file_id(self) -> None:
        files = (ESFile(file_id="f-1"), ESFile(file_id="f-0"))
        builder = self._arrange_builder(files, ())

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(
            result_df.where(F.col("entity") == F.lit("case")).collect()
        )

        assert result_row.file_id == "f-0"

    def test__build_from_scratch__aliquot_none(self) -> None:
        builder = self._arrange_builder((ESFile(),), ())

        result_df = builder.build_from_scratch()
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
    def test__build_from_scratch__aliquot_created_datetime(
        self, primary_datetime: datetime.datetime, other_datetime: datetime.datetime
    ) -> None:
        aliquots = (
            ESAliquot(
                aliquot_id="a-0",
                created_datetime=other_datetime.isoformat(timespec="microseconds"),
            ),
            ESAliquot(
                aliquot_id="a-1",
                created_datetime=primary_datetime.isoformat(timespec="microseconds"),
            ),
        )
        sample = ESSample(
            portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-1" for row in result_rows)

    def test__build_from_scratch__aliquot_id(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()
        result_rows = result_df.collect()

        assert all(row.aliquot_id == "a-0" for row in result_rows)

    def test__build_from_scratch__missing_analytes(self) -> None:
        aliquots = (ESAliquot(aliquot_id="a-0"), ESAliquot(aliquot_id="a-1"))
        sample = ESSample(
            portions=(
                ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),
                ESPortion(analytes=None),
            )
        )
        file = ESFile(cases=(ESCase(samples=(sample,)),))
        builder = self._arrange_builder((file,))

        result_df = builder.build_from_scratch()

        assert result_df.count() == 2


class TestPrimaryAliquotBuilderOLD(unittest.TestCase):
    schema_validator = schema_validation.PysparkSchemaValidator()

    @pytest.fixture(autouse=True)
    def fixture_set_up(self, spark_session: sql.SparkSession, data_dir: str):
        self.sql_context = sql.SQLContext(spark_session.sparkContext)
        self.data_dir = data_dir

    def _build_es_dataframe(self, data: Iterable[dict], is_gene_expression: bool):
        sample_fields = [
            types.StructField("sample_id", types.StringType(), False),
            types.StructField("sample_type", types.StringType(), False),
        ]

        if not is_gene_expression:
            sample_fields.append(
                types.StructField(
                    "portions",
                    types.ArrayType(
                        types.StructType(
                            [
                                types.StructField(
                                    "analytes",
                                    types.ArrayType(
                                        types.StructType(
                                            [
                                                types.StructField(
                                                    "aliquots",
                                                    types.ArrayType(
                                                        types.StructType(
                                                            [
                                                                types.StructField(
                                                                    "aliquot_id",
                                                                    types.StringType(),
                                                                ),
                                                                types.StructField(
                                                                    "created_datetime",
                                                                    types.StringType(),
                                                                ),
                                                            ]
                                                        )
                                                    ),
                                                )
                                            ]
                                        )
                                    ),
                                )
                            ]
                        )
                    ),
                )
            )

        min_case_fields = [
            types.StructField("case_id", types.StringType(), False),
            types.StructField(
                "samples",
                types.ArrayType(types.StructType(sample_fields)),
                False,
            ),
        ]
        extra_case_fields = [
            types.StructField("submitter_id", types.StringType(), False),
            types.StructField(
                "demographic",
                types.StructType(
                    [
                        types.StructField("days_to_death", types.IntegerType(), False),
                        types.StructField("ethnicity", types.StringType(), False),
                        types.StructField("gender", types.StringType(), False),
                        types.StructField("race", types.StringType(), False),
                        types.StructField("vital_status", types.StringType(), False),
                    ]
                ),
                False,
            ),
            types.StructField(
                "project",
                types.StructType(
                    [
                        types.StructField("project_id", types.StringType(), False),
                    ]
                ),
                False,
            ),
            types.StructField(
                "diagnoses",
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField(
                                "age_at_diagnosis", types.IntegerType(), False
                            )
                        ]
                    )
                ),
                False,
            ),
        ]
        case_fields = (
            min_case_fields + extra_case_fields
            if is_gene_expression
            else min_case_fields
        )
        schema = types.StructType(
            [
                types.StructField("file_id", types.StringType(), False),
                types.StructField("experimental_strategy", types.StringType(), False),
                types.StructField("created_datetime", types.StringType(), False),
                types.StructField(
                    "cases", types.ArrayType(types.StructType(case_fields)), False
                ),
            ]
        )

        return self.sql_context.createDataFrame(data, schema)

    def _load_data_from_file(self, filename: str):
        with open(path.join(self.data_dir, filename)) as f:
            return yaml.safe_load(f)

    def _load_data_into_df(self, filename: str, is_gene_expression: bool = False):
        data = self._load_data_from_file(filename)

        return self._build_es_dataframe(data, is_gene_expression)

    @staticmethod
    def _mock_bulk_request(bids: Iterable[str]):
        docs = collections.defaultdict(
            mock.MagicMock
        )  # type: Dict[str, mock.MagicMock]
        docs["file-2"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
            (
                "s3://cleversafe.service.consul/good_url",
                {"type": "cleversafe", "state": "validated"},
            ),
        )
        docs["file-2"].did = "file-2"
        docs["file-4"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            (
                "s3://cleversafe.service.consul/good_url",
                {"type": "cleversafe", "state": "validated"},
            ),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-4"].did = "file-4"
        docs["file-8"].urls_metadata.items.return_value = (
            ("bad_url", {"type": "aws", "state": "validated"}),
            ("bad_url", {"type": "cleversafe", "state": "unvalidated"}),
        )
        docs["file-8"].did = "file-8"

        return (doc for file_id, doc in docs.items() if file_id in bids)

    def test__build_gene_expression_prinary_aliquot_data(self):
        # Arrange
        config = mock.MagicMock()
        config.projects = ["TEST0", "TEST1"]
        config.indexd.bulk_request.side_effect = self._mock_bulk_request
        es_dataframe_util = mock.MagicMock()
        es_df = self._load_data_into_df(
            "input/test_primry_aliquot_builder_common.yaml", True
        )
        es_dataframe_util.get_dataframe.return_value = es_df
        primary_aliquot_builder = builders.PrimaryAliquotBuilder(
            config, self.sql_context, config.indexd, es_dataframe_util, mock.MagicMock()
        )

        primary_aliquot_builder.logger = mock.MagicMock()
        primary_aliquot_builder.FILE_URL_BATCH_SIZE = 3

        # Load Expected Results
        expected = self._load_data_from_file(
            "output/test_build_gene_expression_prinary_aliquot_data.yaml"
        )
        expected_es_include_fields = frozenset(expected["expected_es_include_fields"])
        expected_es_query = expected["expected_es_query"]
        expected_data = {row["case_id"]: row for row in expected["expected_data"]}
        expected_schema = schema_validation.Schema(expected["expected_schema"])

        # Act
        result = primary_aliquot_builder.build_gene_expression_primary_aliquot_data(
            ["type0", "type1"]
        )
        result_df = result.primary_aliquot_df
        result_urls = result.file_urls

        # Assert
        # Check External Calls
        es_dataframe_util.get_dataframe.assert_called_once_with(
            build.IndexType.FILE,
            include_fields=expected_es_include_fields,
            query=expected_es_query,
        )
        self.assertEqual(config.indexd.bulk_request.call_count, 2)
        primary_aliquot_builder.logger.warning.assert_has_calls(
            [
                mock.call("File is missing: 'file-8'"),
            ],
            any_order=True,
        )

        # Check Results
        self.schema_validator.validate_schema(result_df.schema, expected_schema)

        result_collected = tuple(row.asDict(True) for row in result_df.collect())
        result_data = {row["case_id"]: row for row in result_collected}

        self.assertEqual(len(result_collected), len(expected_data))
        self.assertSetEqual(set(result_data.keys()), set(expected_data.keys()))
        self.assertDictEqual(result_data, expected_data)

        self.assertEqual(len(result_urls), 2)
        self.assertSetEqual(set(result_urls), {"s3a://good_url"})
