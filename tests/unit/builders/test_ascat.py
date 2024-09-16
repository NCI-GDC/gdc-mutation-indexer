import dataclasses
import unittest
from collections.abc import Iterable, Mapping
from unittest import mock

import more_itertools
from pyspark import sql

from mutation_indexer import builders
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@dataclasses.dataclass(frozen=True)
class AscatDocument:
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


DEFAULT_ASCAT_DOCUMENTS = (
    AscatDocument(),
    AscatDocument(copy_number=30),
    AscatDocument(copy_number=30),
)


def _arrange_dataframe_util(dataframe: sql.DataFrame) -> mock.MagicMock:
    dataframe_util = mock.MagicMock()

    dataframe_util.get_dataframe.return_value = dataframe

    return dataframe_util


class TestAscatBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._input_ascat_schema = schemas.Viz.Builders.ASCAT.DOCUMENT.load()
        cls._input_gene_model_schema = schemas.Builders.GeneModel.FINAL.load()
        cls._ascat_metadata_schema = schemas.Viz.Builders.ASCATMetadata.FINAL.load()
        cls._final_ascat_schema = schemas.Viz.Builders.ASCAT.FINAL.load()

    def _arrange_doc_dataframe_util(
        self, ascat_document_data: tuple[AscatDocument, ...]
    ) -> mock.MagicMock:
        ascat_document_df = utils.create_dataframe(
            ascat_document_data, self._input_ascat_schema
        )

        return _arrange_dataframe_util(ascat_document_df)

    def _arrange_builder(
        self,
        ascat_documents: tuple[AscatDocument, ...] = DEFAULT_ASCAT_DOCUMENTS,
    ) -> builders.ASCATBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")
        config = mock.MagicMock(
            spec=viz.ASCATBuilder,
            omit_cnv_data=False,
            is_cached=False,
            backup=backup,
            projects=(),
            acl=(),
        )
        mock_sql_context = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(ascat_documents)

        return builders.ASCATBuilder(config, mock_sql_context, doc_dataframe_util)

    def _arrange_input_dataframes(
        self,
        metadata: tuple[Metadata, ...] = (Metadata(),),
        gene_model: tuple[models.GeneModel, ...] = (models.GeneModel(),),
    ) -> Mapping[str, sql.DataFrame]:
        ascat_metadata_df = utils.create_dataframe(
            metadata, self._ascat_metadata_schema
        )
        gene_model_df = utils.create_dataframe(
            gene_model, self._input_gene_model_schema
        )

        return {
            "ascat_metadata_df": ascat_metadata_df,
            "gene_model_df": gene_model_df,
        }

    def test__build__joins_single_record(self) -> None:
        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder()

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 1
        assert ascat_df.schema == self._final_ascat_schema

    def test__build__input_data_transformed(self) -> None:
        metadata = Metadata()
        gene_model = models.GeneModel()

        inputs = self._arrange_input_dataframes((metadata,))
        builder = self._arrange_builder()

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.aliquot_id == metadata.aliquot_id
        assert ascat_row.biotype == gene_model.biotype
        assert ascat_row.case_id == metadata.case_id
        assert ascat_row.end_position == gene_model.gene_end
        assert ascat_row.gene_chromosome == gene_model.chromosome
        assert ascat_row.src_file_id == metadata.file_id
        assert ascat_row.start_position == gene_model.gene_start
        assert ascat_row.symbol == gene_model.symbol
        assert ascat_row.variant_caller == metadata.workflow_type

    @utils.parametrize(
        missing_es_file=(
            (Metadata(file_id="file-1"),),
            DEFAULT_ASCAT_DOCUMENTS,
            (models.GeneModel(),),
        ),
        missing_ascat_document=(
            (Metadata(),),
            (
                AscatDocument(did="file-1"),
                AscatDocument(did="file-1", copy_number=30),
                AscatDocument(did="file-1", copy_number=30),
            ),
            (models.GeneModel(),),
        ),
        missing_gene_model_record=(
            (Metadata(),),
            DEFAULT_ASCAT_DOCUMENTS,
            (models.GeneModel(_gene_id="ENSG00000238008"),),
        ),
    )
    def test__build__failed_joins(
        self,
        metadata: tuple[Metadata, ...],
        ascat_documents: tuple[AscatDocument, ...],
        gene_model: tuple[models.GeneModel, ...],
    ) -> None:
        inputs = self._arrange_input_dataframes(metadata, gene_model)
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0
        assert ascat_df.schema == self._final_ascat_schema

    def test__build__gene_id_stripped(self) -> None:
        ascat_documents = (
            AscatDocument(gene_id="ENSG00000238009.9"),
            AscatDocument(copy_number=30),
            AscatDocument(copy_number=30),
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.gene_id == "ENSG00000238009"

    @utils.parametrize[tuple[int, ...], str](
        ((100, 50, 50), "Gain"),
        ((1, 0, 0), "Gain"),
        ((100, 200, 200), "Loss"),
        ((0, 2, 2), "Loss"),
        ((40, 20, 20, 30, 30), "Gain"),
        ((1, 2, 2, 3, 3), "Loss"),
    )
    def test__build__copy_number_maps_to_cnv_change(
        self, copy_numbers: tuple[int, ...], cnv_change: str
    ) -> None:
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes()
        builder = self._arrange_builder(ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.cnv_change == cnv_change

    @utils.parametrize[Iterable[int]](
        ((30,),), ((31, 32, 33),), ((33, 20, 20, 40, 40),)
    )
    def test__build__neutral_copy_numbers_filtered(
        self, copy_numbers: Iterable[int]
    ) -> None:
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
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
        canonical_transcript = models.Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = models.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gene_model = (
            models.GeneModel(transcripts=(other_transcript, canonical_transcript)),
        )

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == canonical_transcript.length
        assert (
            result_row.canonical_transcript_length_cds
            == canonical_transcript.length_cds
        )
        assert (
            canonical_transcript.end is not None
            and canonical_transcript.start is not None
        )
        assert (
            result_row.canonical_transcript_length_genomic
            == canonical_transcript.end - canonical_transcript.start + 1
        )

    def test__build__null_canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = models.Transcript(
            length=None, length_cds=None, end=None, start=None, is_canonical=True
        )
        other_transcript = models.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gene_model = (
            models.GeneModel(transcripts=(other_transcript, canonical_transcript)),
        )

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    def test__build__canonical_transcript_lengths_no_canonical_transcript(
        self,
    ) -> None:
        transcript = models.Transcript(length=10, length_cds=3, end=122, start=100)
        gene_model = (models.GeneModel(transcripts=(transcript,)),)

        inputs = self._arrange_input_dataframes(gene_model=gene_model)
        builder = self._arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    @utils.parametrize(
        non_protein_coding=(
            models.GeneModel(biotype="transcribed_unprocessed_pseudogene"),
            (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
        ),
        gm_x_chromosome=(
            models.GeneModel(chromosome="X"),
            (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
        ),
        ascat_x_chromosome=(
            models.GeneModel(),
            (
                AscatDocument(copy_number=30, chromosome="X"),
                AscatDocument(),
                AscatDocument(),
            ),
        ),
    )
    def test__build__filter_gene_model(
        self, gene_model: models.GeneModel, ascat_documents: tuple[AscatDocument, ...]
    ) -> None:
        inputs = self._arrange_input_dataframes(gene_model=(gene_model,))
        builder = self._arrange_builder(ascat_documents)

        result_df = builder.build(**inputs)

        assert result_df.count() == 0
