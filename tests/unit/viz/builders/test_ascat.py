import dataclasses
import itertools
from collections.abc import Iterable, Mapping
from unittest import mock

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
class CNVDatum:
    did: str = "file-0"
    gene_id: str = "ENSG00000238009"
    gene_name: str = "RP11-34P13.7"
    chromosome: str = "chr1"
    start: int = 0
    end: int = 100
    copy_number: int = 33
    min_copy_number: int = 0
    max_copy_number: int = 100


@dataclasses.dataclass(frozen=True)
class Metadata:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "ASCAT3"
    analysis_id: str = "analysis-0"


DEFAULT_CNV_DATA = (
    CNVDatum(),
    CNVDatum(copy_number=30),
    CNVDatum(copy_number=30),
)


def _arrange_dataframe_util(dataframe: sql.DataFrame) -> mock.MagicMock:
    dataframe_util = mock.MagicMock()

    dataframe_util.get_dataframe.return_value = dataframe

    return dataframe_util


@pytest.fixture(scope="class")
def input_ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.DOCUMENT.load()


@pytest.fixture(scope="class")
def input_gene_model_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


@pytest.fixture(scope="class")
def ascat_metadata_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCATMetadata.FINAL.load()


@pytest.fixture(scope="class")
def final_ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


class TestAscatBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        input_ascat_schema: types.StructType,
        ascat_metadata_schema: types.StructType,
        input_gene_model_schema: types.StructType,
        final_ascat_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.input_ascat_schema = input_ascat_schema
        self.ascat_metadata_schema = ascat_metadata_schema
        self.input_gene_model_schema = input_gene_model_schema
        self.final_ascat_schema = final_ascat_schema

    def _arrange_doc_dataframe_util(self, cnv_data: tuple[CNVDatum, ...]) -> mock.MagicMock:
        ascat_document_df = self.create_dataframe(cnv_data, self.input_ascat_schema)

        return _arrange_dataframe_util(ascat_document_df)

    def _arrange_builder(
        self,
        cnv_data: tuple[CNVDatum, ...] = DEFAULT_CNV_DATA,
    ) -> builders.ASCATBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")
        config = mock.MagicMock(
            spec=configuration.ASCATBuilder,
            omit_cnv_data=False,
            is_cached=False,
            backup=backup,
            projects=(),
            acl=(),
        )
        mock_sql_context = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(cnv_data)

        return builders.ASCATBuilder(config, mock_sql_context, doc_dataframe_util)

    def _arrange_input_dataframes(
        self,
        metadata: tuple[Metadata, ...] = (Metadata(),),
        gene_model: tuple[models.GeneModel, ...] = (models.GeneModel(),),
    ) -> Mapping[str, sql.DataFrame]:
        ascat_metadata_df = self.create_dataframe(metadata, self.ascat_metadata_schema)
        gene_model_df = self.create_dataframe(gene_model, self.input_gene_model_schema)

        return {
            "ascat_metadata_df": ascat_metadata_df,
            "gene_model_df": gene_model_df,
        }

    def test__build__joins_single_record(self) -> None:
        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder()

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 1
        assert ascat_df.schema == self.final_ascat_schema

    def test__build__input_data_transformed(self) -> None:
        metadata = Metadata()
        gene_model = models.GeneModel()
        ascat = DEFAULT_CNV_DATA[0]

        inputs = self._arrange_input_dataframes((metadata,))
        builder = self._arrange_builder(cnv_data=DEFAULT_CNV_DATA)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.aliquot_id == metadata.aliquot_id
        assert ascat_row.biotype == gene_model.biotype
        assert ascat_row.case_id == metadata.case_id
        assert ascat_row.copy_number == ascat.copy_number
        assert ascat_row.end_position == gene_model.gene_end
        assert ascat_row.gene_chromosome == gene_model.chromosome
        assert ascat_row.src_file_id == metadata.file_id
        assert ascat_row.start_position == gene_model.gene_start
        assert ascat_row.symbol == gene_model.symbol
        assert ascat_row.variant_caller == metadata.workflow_type

    @pytest.mark.parametrize(
        ("metadata", "ascat_documents", "gene_model"),
        (
            (
                (Metadata(file_id="file-1"),),
                DEFAULT_CNV_DATA,
                (models.GeneModel(),),
            ),
            (
                (Metadata(),),
                (
                    CNVDatum(did="file-1"),
                    CNVDatum(did="file-1", copy_number=30),
                    CNVDatum(did="file-1", copy_number=30),
                ),
                (models.GeneModel(),),
            ),
            (
                (Metadata(),),
                DEFAULT_CNV_DATA,
                (models.GeneModel(_gene_id="ENSG00000238008"),),
            ),
        ),
        ids=(
            "missing_es_file",
            "missing_ascat_document",
            "missing_gene_model_record",
        ),
    )
    def test__build__failed_joins(
        self,
        metadata: tuple[Metadata, ...],
        ascat_documents: tuple[CNVDatum, ...],
        gene_model: tuple[models.GeneModel, ...],
    ) -> None:
        inputs = self._arrange_input_dataframes(metadata, gene_model)
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0
        assert ascat_df.schema == self.final_ascat_schema

    def test__build__zero_ploidy_documents_removed(self) -> None:
        ascat_documents = tuple(
            CNVDatum(copy_number=copy_number) for copy_number in (0, 0, 0, 0, 2)
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0

    @pytest.mark.parametrize(
        ("copy_numbers", "expected"),
        (
            pytest.param((2, 2, 3), 2, id="single-mode-value"),
            pytest.param((2, 2, 5, 5, 1), 4, id="multiple-mode-values"),
        ),
    )
    def test__build__mean_sample_ploidy_added(
        self, copy_numbers: Iterable[int], expected: int
    ) -> None:
        ascat_documents = tuple(
            CNVDatum(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.sample_ploidy_integer == expected

    def test__build__gene_id_stripped(self) -> None:
        ascat_documents = (
            CNVDatum(gene_id="ENSG00000238009.9"),
            CNVDatum(copy_number=30),
            CNVDatum(copy_number=30),
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.gene_id == "ENSG00000238009"

    @pytest.mark.parametrize(
        ("copy_numbers", "cnv_change"),
        (
            ((100, 50, 50), "Gain"),
            ((5, 1, 1), "Gain"),
            ((100, 200, 200), "Loss"),
            ((-1, 2, 2), "Loss"),
            ((40, 20, 20, 30, 30), "Gain"),
            ((1, 2, 2, 3, 3), "Loss"),
        ),
    )
    def test__build__copy_number_maps_to_cnv_change(
        self, copy_numbers: tuple[int, ...], cnv_change: str
    ) -> None:
        ascat_documents = tuple(
            CNVDatum(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.cnv_change == cnv_change

    @pytest.mark.parametrize(
        ("copy_numbers", "cnv_change_5_category"),
        (
            pytest.param((200, 50, 50), "Amplification", id="amplification_single_mode"),
            pytest.param(
                (200, 50, 50, 30, 30), "Amplification", id="amplification_multiple_mode"
            ),
            pytest.param((75, 50, 50), "Gain", id="gain_single_mode"),
            pytest.param((75, 50, 50, 30, 30), "Gain", id="gain_multiple_mode"),
            pytest.param((50, 50, 0), "Homozygous Deletion", id="deletion_single_mode"),
            pytest.param(
                (50, 50, 20, 20, 0), "Homozygous Deletion", id="deletion_multiple_mode"
            ),
            pytest.param((100, 200, 200), "Loss", id="loss_single_mode"),
            pytest.param((100, 200, 200, 300, 300), "Loss", id="loss_multiple_mode"),
        ),
    )
    def test__build__copy_number_maps_to_cnv_change_5_category(
        self, copy_numbers: tuple[int, ...], cnv_change_5_category: str
    ) -> None:
        ascat_documents = tuple(
            CNVDatum(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.cnv_change_5_category == cnv_change_5_category

    def test__build__only_include_chr1_to_22_and_protein_coding_genes_for_ploidy(
        self,
    ) -> None:
        # Only these genes should be used to calculate the cnv change. With a mode value
        # of 2, the only cnv generated should be a loss.
        valid_genes = (
            CNVDatum(gene_id="chr4", copy_number=2),
            CNVDatum(gene_id="chr13", copy_number=2),
            CNVDatum(gene_id="chr21", copy_number=1),
        )
        # These genes should be filtered before calculation.
        non_protein_coding_genes = itertools.repeat(
            CNVDatum(gene_id="non-protein-coding", copy_number=0), 3
        )
        x_genes = itertools.repeat(CNVDatum(gene_id="X", copy_number=0), 3)
        y_genes = itertools.repeat(CNVDatum(gene_id="Y", copy_number=0), 3)
        ascat_data = (
            *valid_genes,
            *non_protein_coding_genes,
            *x_genes,
            *y_genes,
        )

        inputs = self._arrange_input_dataframes(
            gene_model=(
                models.GeneModel(_gene_id="non-protein_coding", biotype="non-protein-coding"),
                models.GeneModel(_gene_id="X", chromosome="X"),
                models.GeneModel(_gene_id="Y", chromosome="Y"),
                *(
                    models.GeneModel(_gene_id=f"chr{i}", chromosome=str(i))
                    for i in range(1, 23)
                ),
            )
        )
        builder = self._arrange_builder(cnv_data=ascat_data)

        ascat_df = builder.build(**inputs)
        assert ascat_df.count() == 1

        ascat_row = ascat_df.first()
        assert ascat_row and ascat_row.cnv_change_5_category == "Loss"

    @pytest.mark.parametrize(
        "copy_numbers",
        ((30,), (31, 32, 33), (33, 20, 20, 40, 40)),
    )
    def test__build__neutral_copy_numbers_filtered(self, copy_numbers: Iterable[int]) -> None:
        ascat_documents = tuple(
            CNVDatum(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0

    def test__build__uuids_generated(self) -> None:
        metadata = Metadata()
        gene_model = models.GeneModel()

        inputs = self._arrange_input_dataframes((metadata,), (gene_model,))
        builder = self._arrange_builder()

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())
        cnv_id = utils.generate_uuid5(
            gene_model.chromosome,
            gene_model.gene_start,
            gene_model.gene_end,
            "Gain",
        )

        assert ascat_row.cnv_id == cnv_id
        assert ascat_row.consequence_id == utils.generate_uuid5(
            gene_model.symbol,
            gene_model._gene_id,
            gene_model.is_cancer_gene_census,
            gene_model.biotype,
        )
        assert ascat_row.occurrence_id == utils.generate_uuid5(cnv_id, metadata.case_id)
        assert ascat_row.observation_id == utils.generate_uuid5(
            cnv_id, metadata.case_id, metadata.aliquot_id
        )

    def test__build__canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = models.GeneModel.Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = models.GeneModel.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gene_model = (models.GeneModel(transcripts=(other_transcript, canonical_transcript)),)

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == canonical_transcript.length
        assert result_row.canonical_transcript_length_cds == canonical_transcript.length_cds
        assert canonical_transcript.end is not None and canonical_transcript.start is not None
        assert (
            result_row.canonical_transcript_length_genomic
            == canonical_transcript.end - canonical_transcript.start + 1
        )

    def test__build__null_canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = models.GeneModel.Transcript(
            length=None, length_cds=None, end=None, start=None, is_canonical=True
        )
        other_transcript = models.GeneModel.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gene_model = (models.GeneModel(transcripts=(other_transcript, canonical_transcript)),)

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length is None
        assert result_row.canonical_transcript_length_cds is None
        assert result_row.canonical_transcript_length_genomic is None

    def test__build__canonical_transcript_lengths_no_canonical_transcript(
        self,
    ) -> None:
        transcript = models.GeneModel.Transcript(length=10, length_cds=3, end=122, start=100)
        gene_model = (models.GeneModel(transcripts=(transcript,)),)

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length is None
        assert result_row.canonical_transcript_length_cds is None
        assert result_row.canonical_transcript_length_genomic is None

    @pytest.mark.parametrize(
        ("gene_model", "ascat_documents"),
        (
            (
                models.GeneModel(biotype="transcribed_unprocessed_pseudogene"),
                (CNVDatum(copy_number=30), CNVDatum(), CNVDatum()),
            ),
            (
                models.GeneModel(chromosome="X"),
                (CNVDatum(copy_number=30), CNVDatum(), CNVDatum()),
            ),
            (
                models.GeneModel(),
                (
                    CNVDatum(copy_number=30, chromosome="X"),
                    CNVDatum(),
                    CNVDatum(),
                ),
            ),
        ),
        ids=("non_protein_coding", "gm_x_chromosome", "ascat_x_chromosome"),
    )
    def test__build__filter_gene_model(
        self, gene_model: models.GeneModel, ascat_documents: tuple[CNVDatum, ...]
    ) -> None:
        inputs = self._arrange_input_dataframes(gene_model=(gene_model,))
        builder = self._arrange_builder(ascat_documents)

        result_df = builder.build(**inputs)

        assert result_df.count() == 0
