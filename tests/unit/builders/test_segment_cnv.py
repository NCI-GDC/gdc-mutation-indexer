import dataclasses
from collections.abc import Mapping
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class SegmentCNVMetadataInputData:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "AscatNGS"
    analysis_id: str = "analysis-0"


@dataclasses.dataclass(frozen=True)
class SegmentCNVDocumentData:
    did: str = "file-0"
    aliquot_id: str = "aliquot-0"
    chromosome: str = "chr1"
    start: int = 50
    end: int = 100
    copy_number: int = 3
    major_copy_number: int = 2
    minor_copy_number: int = 1


@pytest.fixture(scope="class")
def segment_cnv_metadata_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNVMetadata.FINAL.load()


@pytest.fixture(scope="class")
def document_segment_cnv_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNV.DOCUMENT.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.SegmentCNV.FINAL.load()


class TestSegmentCNVBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        segment_cnv_metadata_schema: types.StructType,
        document_segment_cnv_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self._create_dataframe = create_dataframe
        self._segment_cnv_metadata_schema = segment_cnv_metadata_schema
        self._document_segment_cnv_schema = document_segment_cnv_schema
        self._final_schema = final_schema

    def _arrange_doc_dataframe_util(
        self, segment_cnv_data: tuple[SegmentCNVDocumentData, ...]
    ) -> mock.MagicMock:
        segment_cnv_data_df = self._create_dataframe(
            segment_cnv_data, self._document_segment_cnv_schema
        )
        mock_dataframe_util = mock.MagicMock()
        mock_dataframe_util.get_dataframe.return_value = segment_cnv_data_df

        return mock_dataframe_util

    def _arrange_input_dataframes(
        self, segment_cnv_metadata: tuple[SegmentCNVMetadataInputData, ...]
    ) -> Mapping[str, sql.DataFrame]:
        segment_cnv_metadata_df = self._create_dataframe(
            segment_cnv_metadata, self._segment_cnv_metadata_schema
        )

        return {"segment_cnv_metadata_df": segment_cnv_metadata_df}

    def _arrange_config(self) -> mock.MagicMock:
        return mock.MagicMock(
            acl=("open",),
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
            spec=viz.Builder,
        )

    def _arrange_builder(
        self, segment_cnv_data: tuple[SegmentCNVDocumentData, ...]
    ) -> builders.SegmentCNVBuilder:
        config = self._arrange_config()
        mock_spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(segment_cnv_data)
        builder = builders.SegmentCNVBuilder(
            config, mock_spark_session, doc_dataframe_util
        )

        return builder

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    def test__build__single_row(self) -> None:
        """Test joining segment_cnv_metadata dataframe with segment cnv file data.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with one result and correct schema.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(
            segment_cnv_data=segment_cnv_data,
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        assert segment_cnv_df.schema == self._final_schema

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    def test__build__input_data_transformed(self) -> None:
        """Test the correctness of the output segment cnv dataframe.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with correct segment copy number file data
            and derived fields.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        assert segment_cnv_df.schema == self._final_schema
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.chromosome == "chr1"
        assert result_row.length == 51
        assert result_row.start_position == 50
        assert result_row.end_position == 100
        assert result_row.cnv_change == "Gain"
        assert result_row.cnv_change_5_category == "Amplification"

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    def test__build__uuids_generated(self) -> None:
        """Test the correctness of the generated uuids.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with the expected generated uuids.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        segment_cnv_id = utils.generate_uuid5("chr1", 50, 100, "Amplification")
        assert result_row.segment_cnv_id == segment_cnv_id
        assert result_row.occurrence_id == utils.generate_uuid5(
            segment_cnv_id, "case-0"
        )
        assert result_row.observation_id == utils.generate_uuid5(
            segment_cnv_id, "case-0", "aliquot-0"
        )

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    def test__build__calculate_length_weighted_mode(self) -> None:
        """Test the correctness of the length-weighted mode calculation.

        Given a segment_cnv_metadata dataframew ith one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with the correct cnv_change_5_category value,
            implying the calculated length-weighted mode is correct.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        copy_numbers, starts, ends = (3, 6), (10, 10), (15, 100)
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number, start=start, end=end)
            for copy_number, start, end in zip(copy_numbers, starts, ends)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.cnv_change_5_category == "Loss"

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    @pytest.mark.parametrize(
        ("copy_numbers", "cnv_change"),
        (
            pytest.param(
                (3, 5, 5),
                "Loss",
                id="loss_single_mode",
            ),
            pytest.param(
                (3, 5, 5, 10, 10),
                "Loss",
                id="loss_multiple_mode",
            ),
            pytest.param(
                (3, 3, 5),
                "Gain",
                id="loss_single_mode",
            ),
            pytest.param(
                (3, 3, 5, 5, 7),
                "Gain",
                id="loss_single_mode",
            ),
        ),
    )
    def test__build__copy_number_to_cnv_change(
        self,
        copy_numbers: tuple[int, ...],
        cnv_change: tuple[str, ...],
    ) -> None:
        """Test to ensure proper cnv_change calculations.

        Given a collection of copy number values and expected cnv_change value
        When SegmentCNVBuilder build is called
        Then the dataframe is returned with the correct cnv_change value.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number)
                for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.cnv_change == cnv_change

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    @pytest.mark.parametrize(
        ("copy_numbers", "cnv_change_5_category"),
        (
            pytest.param(
                (0, 5, 5),
                "Homozygous Deletion",
                id="deletion_single_mode",
            ),
            pytest.param(
                (0, 5, 5, 10, 10),
                "Homozygous Deletion",
                id="deletion_multiple_mode",
            ),
            pytest.param(
                (5, 10, 10),
                "Loss",
                id="loss_single_mode",
            ),
            pytest.param(
                (3, 5, 5, 10, 10),
                "Loss",
                id="loss_multiple_mode",
            ),
            pytest.param(
                (5, 5, 7),
                "Gain",
                id="gain_single_mode",
            ),
            pytest.param(
                (5, 5, 7, 7, 9),
                "Gain",
                id="gain_multiple_mode",
            ),
            pytest.param(
                (5, 5, 10),
                "Amplification",
                id="amplification_single_mode",
            ),
            pytest.param(
                (5, 5, 10, 10, 20),
                "Amplification",
                id="amplification_multiple_mode",
            ),
        ),
    )
    def test__build__copy_number_to_cnv_change_5_category(
        self,
        copy_numbers: tuple[int, ...],
        cnv_change_5_category: str,
    ) -> None:
        """Test to ensure proper cnv_change_5_category calculations.

        Given a collection of copy number values and expected cnv_change_5_category value
        When SegmentCNVBuilder build is called
        Then the dataframe is returned with the correct cnv_change_5_category value.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number)
                for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.cnv_change_5_category == cnv_change_5_category

    @pytest.mark.xfail(reason="SegmentCNVBuilder not implemented yet")
    def test__build__filter_neutral_values(self) -> None:
        """Test to ensure cnv_change_5_category neutral rows are filtered.

        Given segment cnv data where the cnv_change_5_category is equal to the mode
        When SegmentCNVBuilder build is called
        Then the dataframe is returned with zero rows.
        """
        copy_numbers = (3, 3, 3)
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(SegmentCNVMetadataInputData(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number)
                for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 0
