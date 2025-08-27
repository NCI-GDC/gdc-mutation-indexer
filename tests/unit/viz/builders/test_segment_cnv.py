import dataclasses
from collections.abc import Mapping
from unittest import mock

import deepdiff
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.constants import build
from mutation_indexer.viz import builders, configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@dataclasses.dataclass(frozen=True)
class SegmentCNVDocumentData:
    did: str = "file-0"
    aliquot_id: str = "aliquot-0"
    chromosome: str = "chr1"
    start_position: int = 50
    end_position: int = 100
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
        self, segment_cnv_metadata: tuple[models.SegmentCNVMetadata, ...]
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
            spec=configuration.SegmentCNVMetadataBuilder,
        )

    def _arrange_builder(
        self, segment_cnv_data: tuple[SegmentCNVDocumentData, ...]
    ) -> builders.SegmentCNVBuilder:
        config = self._arrange_config()
        mock_spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(segment_cnv_data)
        builder = builders.SegmentCNVBuilder(config, mock_spark_session, doc_dataframe_util)

        return builder

    def test__build__single_row(self) -> None:
        """Test joining segment_cnv_metadata dataframe with segment cnv file data.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with one result and correct schema.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(
                models.SegmentCNVMetadata(file_id="file-0"),
                models.SegmentCNVMetadata(file_id="file-1"),
            )
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(
            segment_cnv_data=segment_cnv_data,
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        assert not deepdiff.DeepDiff(
            segment_cnv_df.schema, self._final_schema, ignore_order=True
        )

    def test__build__input_data_transformed(self) -> None:
        """Test the correctness of the output segment cnv dataframe.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with correct segment copy number file data
            and derived fields.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        assert not deepdiff.DeepDiff(
            segment_cnv_df.schema, self._final_schema, ignore_order=True
        )
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.src_file_id == "file-0"
        assert result_row.copy_number == 6
        assert result_row.chromosome == "1"
        assert result_row.length == 51
        assert result_row.start_position == 50
        assert result_row.end_position == 100
        assert result_row.cnv_change == "Gain"
        assert result_row.cnv_change_5_category == "Amplification"
        assert result_row.variant_caller == "AscatNGS"
        assert result_row.variant_status == "Tumor Only"

    def test__build__uuids_generated(self) -> None:
        """Test the correctness of the generated uuids.

        Given a segment_cnv_metadata dataframe with one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with the expected generated uuids.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number) for copy_number in (3, 3, 6)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        segment_cnv_id = utils.generate_uuid5("1", 50, 100, "Amplification")
        assert result_row.segment_cnv_id == segment_cnv_id
        assert result_row.occurrence_id == utils.generate_uuid5(segment_cnv_id, "case-0")
        assert result_row.observation_id == utils.generate_uuid5(
            segment_cnv_id, "case-0", "aliquot-0"
        )

    def test__build__calculate_length_weighted_mode(self) -> None:
        """Test the correctness of the length-weighted mode calculation.

        Given a segment_cnv_metadata dataframew ith one segment file metadata
        When SegmentCNVBuilder build is called
        Then return a dataframe with the correct cnv_change_5_category value,
            implying the calculated length-weighted mode is correct.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        copy_numbers, starts, ends = (3, 6, 6, 6), (10, 10, 11, 12), (100, 15, 16, 17)
        segment_cnv_data = tuple(
            SegmentCNVDocumentData(
                copy_number=copy_number, start_position=start, end_position=end
            )
            for copy_number, start, end in zip(copy_numbers, starts, ends)
        )
        builder = self._arrange_builder(segment_cnv_data=segment_cnv_data)
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 3
        result_rows = segment_cnv_df.collect()
        assert all(
            result_row.cnv_change_5_category == "Amplification" for result_row in result_rows
        )

    def test__build__multiple_files(self) -> None:
        """Test calculating correct cnv_change data with multiple files.

        Given a segment_cnv_metadata dataframe with two segment files
        When SegmentCNVBuilder build is called
        Then return a dataframe with two results and correct cnv_change data.
        """
        segment_cnv_metadata = tuple(
            models.SegmentCNVMetadata(file_id=f"file-{i}") for i in range(2)
        )
        inputs = self._arrange_input_dataframes(segment_cnv_metadata=segment_cnv_metadata)
        file0_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number, did="file-0")
            for copy_number in (5, 5, 10, 10, 20)
        )
        file1_data = tuple(
            SegmentCNVDocumentData(copy_number=copy_number, did="file-1")
            for copy_number in (3, 5, 5, 10, 10)
        )
        segment_cnv_data = file0_data + file1_data
        builder = self._arrange_builder(
            segment_cnv_data=segment_cnv_data,
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 2
        file0_row = more_itertools.one(
            segment_cnv_df.where(segment_cnv_df["src_file_id"] == "file-0").collect()
        )
        file1_row = more_itertools.one(
            segment_cnv_df.where(segment_cnv_df["src_file_id"] == "file-1").collect()
        )
        assert file0_row.cnv_change == "Gain"
        assert file0_row.cnv_change_5_category == "Amplification"
        assert file1_row.cnv_change == "Loss"
        assert file1_row.cnv_change_5_category == "Loss"

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
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number) for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.cnv_change == cnv_change

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
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number) for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 1
        result_row = more_itertools.one(segment_cnv_df.collect())

        assert result_row.cnv_change_5_category == cnv_change_5_category

    def test__build__filter_neutral_values(self) -> None:
        """Test to ensure cnv_change_5_category neutral rows are filtered.

        Given segment cnv data where the cnv_change_5_category is equal to the mode
        When SegmentCNVBuilder build is called
        Then the dataframe is returned with zero rows.
        """
        copy_numbers = (3, 3, 3)
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=tuple(
                SegmentCNVDocumentData(copy_number=copy_number) for copy_number in copy_numbers
            )
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 0

    def test__build__filter_chromosomes(self) -> None:
        """Test to filter out chromosomes that don't fall between 1 and 22.

        Given a segment_cnv data where the chromosome is not between 1 and 22
        When SegmentCNVBuilder build is called
        Then the dataframe is returned with zero rows.
        """
        inputs = self._arrange_input_dataframes(
            segment_cnv_metadata=(models.SegmentCNVMetadata(),)
        )
        builder = self._arrange_builder(
            segment_cnv_data=(SegmentCNVDocumentData(chromosome="chr23"),)
        )
        segment_cnv_df = builder.build(**inputs)

        assert segment_cnv_df.count() == 0
