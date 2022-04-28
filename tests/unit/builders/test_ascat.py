import dataclasses
from typing import Dict, Iterable, Mapping, Optional, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from tests.unit import utils
from tests.unit.data import schemas


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
class ESAliquot:
    aliquot_id: str = "aliquot-0"


@dataclasses.dataclass(frozen=True)
class ESAnalyte:
    aliquots: Tuple[ESAliquot, ...] = (ESAliquot(),)


@dataclasses.dataclass(frozen=True)
class ESPortion:
    analytes: Tuple[ESAnalyte, ...] = (ESAnalyte(),)


@dataclasses.dataclass(frozen=True)
class ESSample:
    portions: Tuple[ESPortion, ...] = (ESPortion(),)


@dataclasses.dataclass(frozen=True)
class ESCase:
    case_id: str = "case-0"
    samples: Tuple[ESSample, ...] = (ESSample(),)


@dataclasses.dataclass(frozen=True)
class ESFile:
    file_id: str = "file-0"
    cases: Tuple[ESCase, ...] = (ESCase(),)


@dataclasses.dataclass(frozen=True)
class Domain:
    description: str = "G protein-coupled receptor, rhodopsin-like"
    end: int = 280
    gff_source: str = "pfam"
    hit_name: str = "PF00001"
    interpro_id: str = "IPR000276"
    start: int = 34


@dataclasses.dataclass(frozen=True)
class Exon:
    cdna_coding_end: int = 0
    cdna_coding_start: int = 0
    cdna_end: int = 359
    cdna_start: int = 1
    end: int = 12227
    end_phase: int = -1
    genomic_coding_end: int = 0
    genomic_coding_stairt: int = 0
    genomic_coding_start: int = 0
    start: int = 11869
    start_phase: int = -1


@dataclasses.dataclass(frozen=True)
class Transcript:
    biotype: str = "processed_transcript"
    cdna_coding_end: int = 0
    cdna_coding_start: int = 0
    coding_region_end: int = 0
    coding_region_start: int = 0
    domains: Tuple[Domain, ...] = (Domain(),)
    end: Optional[int] = 14409
    end_exon: Optional[int] = None
    exons: Tuple[Exon, ...] = (Exon(),)
    transcript_id: str = "ENST00000456328"
    is_canonical: bool = False
    length: Optional[int] = 1657
    length_amino_acid: Optional[int] = None
    length_cds: Optional[int] = None
    name: str = "DDX11L1-002"
    number_of_exons: int = 6
    seq_exon_end: Optional[int] = None
    seq_exon_start: Optional[int] = None
    start: Optional[int] = 11869
    start_exon: Optional[int] = None
    translation_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class GeneModel:
    _gene_id: str = "ENSG00000238009"
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"},
    )
    biotype: str = "protein_coding"
    canonical_transcript_id: str = "ENST00000456328"
    chromosome: str = "1"
    cytoband: Tuple[Optional[str], ...] = ("1p36.33",)
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    entrez_gene: Tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    gene_end: int = 14409
    gene_start: int = 11869
    gene_strand: int = 1
    hgnc: Tuple[str, ...] = ("HGNC:37102",)
    is_cancer_gene_census: str = "true"
    omim_gene: Tuple[str, ...] = ()
    uniprotkb_swissprot: Tuple[str, ...] = ()
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    symbol: str = "DDX11L1"
    synonyms: Tuple[str, ...] = ()
    transcripts: Tuple[Transcript, ...] = (Transcript(),)


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    entity: str = "file"
    file_id: str = "file-0"
    aliquot_id: str = "aliquot-0"


DEFAULT_ASCAT_DOCUMENTS = (
    AscatDocument(),
    AscatDocument(copy_number=30),
    AscatDocument(copy_number=30),
)


def _arrange_dataframe_util(dataframe: sql.DataFrame) -> mock.MagicMock:
    dataframe_util = mock.MagicMock()

    dataframe_util.get_dataframe.return_value = dataframe

    return dataframe_util


def _arrange_iterate_es_results_return(file_ids: Iterable[str]) -> Iterable[dict]:
    return ({"_source": {"file_id": file_id}} for file_id in file_ids)


def _arrange_case(
    case_id: str = "case-0", aliquots: Tuple[ESAliquot, ...] = (ESAliquot(),)
) -> ESCase:
    return ESCase(
        case_id=case_id,
        samples=(
            ESSample(portions=(ESPortion(analytes=(ESAnalyte(aliquots=aliquots),)),)),
        ),
    )


@pytest.fixture(scope="class")
def input_ascat_schema() -> types.StructType:
    return schemas.load_schema("builders/ascat/input_ascat.yaml")


@pytest.fixture(scope="class")
def es_file_schema() -> types.StructType:
    return schemas.load_schema("builders/ascat/es_file.json")


@pytest.fixture(scope="class")
def input_gene_model_schema() -> types.StructType:
    return schemas.load_schema("builders/ascat/input_gene_model.json")


@pytest.fixture(scope="class")
def final_ascat_schema() -> types.StructType:
    return schemas.load_schema("builders/ascat/final_ascat.json")


class TestAscatBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        input_ascat_schema: types.StructType,
        es_file_schema: types.StructType,
        input_gene_model_schema: types.StructType,
        final_ascat_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.input_ascat_schema = input_ascat_schema
        self.es_file_schema = es_file_schema
        self.input_gene_model_schema = input_gene_model_schema
        self.final_ascat_schema = final_ascat_schema

    def _arrange_doc_dataframe_util(
        self, ascat_document_data: Tuple[AscatDocument, ...]
    ) -> mock.MagicMock:
        ascat_document_df = self.spark_session.createDataFrame(
            ascat_document_data,
            self.input_ascat_schema,
        )

        return _arrange_dataframe_util(ascat_document_df)

    def _arrange_es_dataframe_util(
        self, es_files: Tuple[ESFile, ...]
    ) -> mock.MagicMock:
        es_file_df = self.spark_session.createDataFrame(es_files, self.es_file_schema)

        return _arrange_dataframe_util(es_file_df)

    def _arrange_builder(
        self,
        es_files: Tuple[ESFile, ...],
        ascat_documents: Tuple[AscatDocument, ...] = DEFAULT_ASCAT_DOCUMENTS,
    ) -> builders.AscatBuilder:
        config = mock.MagicMock()
        mock_sql_context = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(ascat_documents)
        es_dataframe_util = self._arrange_es_dataframe_util(es_files)
        es_client = mock.MagicMock()

        return builders.AscatBuilder(
            config, mock_sql_context, doc_dataframe_util, es_dataframe_util, es_client
        )

    def _arrange_input_dataframes(
        self,
        primary_aliquots: Tuple[PrimaryAliquot, ...],
        gene_model: Tuple[GeneModel, ...],
    ) -> Mapping[str, sql.DataFrame]:
        primary_aliquot_df = self.spark_session.createDataFrame(primary_aliquots)
        gene_model_df = self.spark_session.createDataFrame(
            gene_model, self.input_gene_model_schema
        )

        return {
            "primary_aliquot_df": primary_aliquot_df,
            "gene_model_df": gene_model_df,
        }

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__joins_single_record(
        self,
        iterate_es_results: mock.MagicMock,
    ) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        ascat_df = builder.build_from_scratch(**inputs)

        assert ascat_df.count() == 1
        assert ascat_df.schema == self.final_ascat_schema

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__input_data_transformed(
        self,
        iterate_es_results: mock.MagicMock,
    ) -> None:
        es_file = ESFile()
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = GeneModel()

        inputs = self._arrange_input_dataframes(primary_aliquots, (gene_model,))
        builder = self._arrange_builder((es_file,))

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.biotype == gene_model.biotype
        assert ascat_row.case_id == es_file.cases[0].case_id
        assert ascat_row.end_position == gene_model.gene_end
        assert ascat_row.gene_chromosome == gene_model.chromosome
        assert ascat_row.start_position == gene_model.gene_start
        assert ascat_row.symbol == gene_model.symbol

    @pytest.mark.parametrize(
        argnames=("es_files", "ascat_documents", "primary_aliquots", "gene_model"),
        argvalues=(
            (
                (ESFile(file_id="file-1"),),
                DEFAULT_ASCAT_DOCUMENTS,
                (PrimaryAliquot(),),
                (GeneModel(),),
            ),
            (
                (
                    ESFile(
                        cases=(
                            _arrange_case(
                                aliquots=(ESAliquot(aliquot_id="aliquot-1"),)
                            ),
                        )
                    ),
                ),
                DEFAULT_ASCAT_DOCUMENTS,
                (PrimaryAliquot(),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                (
                    AscatDocument(did="file-1"),
                    AscatDocument(did="file-1", copy_number=30),
                    AscatDocument(did="file-1", copy_number=30),
                ),
                (PrimaryAliquot(),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (PrimaryAliquot(aliquot_id="aliquot-1"),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (PrimaryAliquot(file_id="file-1"),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (PrimaryAliquot(),),
                (GeneModel(_gene_id="ENSG00000238008"),),
            ),
        ),
        ids=(
            "missing_es_file",
            "es_file_non_existant_gene_id",
            "missing_ascat_document",
            "not_primary_aliquot_for_file",
            "es_file_not_in_primary_aliquot",
            "missing_gene_model_record",
        ),
    )
    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__failed_joins(
        self,
        iterate_es_results: mock.MagicMock,
        es_files: Tuple[ESFile, ...],
        ascat_documents: Tuple[AscatDocument, ...],
        primary_aliquots: Tuple[PrimaryAliquot, ...],
        gene_model: Tuple[GeneModel, ...],
    ) -> None:
        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)

        assert ascat_df.count() == 0
        assert ascat_df.schema == self.final_ascat_schema

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__gene_id_stripped(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        es_files = (ESFile(),)
        ascat_documents = (
            AscatDocument(gene_id="ENSG00000238009.9"),
            AscatDocument(copy_number=30),
            AscatDocument(copy_number=30),
        )
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.gene_id == "ENSG00000238009"

    @pytest.mark.parametrize(
        ("copy_numbers", "cnv_change"),
        (
            ((100, 50, 50), "Gain"),
            ((1, 0, 0), "Gain"),
            ((100, 200, 200), "Loss"),
            ((0, 2, 2), "Loss"),
            ((40, 20, 20, 30, 30), "Gain"),
            ((1, 2, 2, 3, 3), "Loss"),
        ),
    )
    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__copy_number_maps_to_cnv_change(
        self,
        iterate_es_results: mock.MagicMock,
        copy_numbers: Tuple[int, ...],
        cnv_change: str,
    ) -> None:
        es_files = (ESFile(),)
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
        )
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.cnv_change == cnv_change

    @pytest.mark.parametrize(
        "copy_numbers",
        ((30,), (31, 32, 33), (33, 20, 20, 40, 40)),
    )
    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__neutral_copy_numbers_filtered(
        self, iterate_es_results: mock.MagicMock, copy_numbers: Iterable[str]
    ) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)

        assert ascat_df.count() == 0

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__uuids_generated(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = GeneModel()

        inputs = self._arrange_input_dataframes(primary_aliquots, (gene_model,))
        builder = self._arrange_builder(es_files)

        ascat_df = builder.build_from_scratch(**inputs)
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
        assert ascat_row.occurrence_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id
        )
        assert ascat_row.observation_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id, primary_aliquots[0].aliquot_id
        )

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__canonical_transcript_lengths_added(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        canonical_transcript = Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = Transcript(length=10, length_cds=3, end=122, start=100)
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(transcripts=(other_transcript, canonical_transcript)),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == canonical_transcript.length
        assert (
            result_row.canonical_transcript_length_cds
            == canonical_transcript.length_cds
        )
        assert (
            result_row.canonical_transcript_length_genomic
            == canonical_transcript.end - canonical_transcript.start + 1
        )

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__null_canonical_transcript_lengths_added(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        canonical_transcript = Transcript(
            length=None, length_cds=None, end=None, start=None, is_canonical=True
        )
        other_transcript = Transcript(length=10, length_cds=3, end=122, start=100)
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(transcripts=(other_transcript, canonical_transcript)),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__canonical_transcript_lengths_no_canonical_transcipt(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        transcript = Transcript(length=10, length_cds=3, end=122, start=100)
        es_files = (ESFile(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(transcripts=(transcript,)),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    @pytest.mark.parametrize(
        ("gene_model", "ascat_documents"),
        (
            (
                GeneModel(biotype="transcribed_unprocessed_pseudogene"),
                (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
            ),
            (
                GeneModel(chromosome="X"),
                (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
            ),
            (
                GeneModel(),
                (
                    AscatDocument(copy_number=30, chromosome="X"),
                    AscatDocument(),
                    AscatDocument(),
                ),
            ),
        ),
        ids=("non_protein_coding", "gm_x_chromosome", "ascat_x_chromosome"),
    )
    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__filter_gene_model(
        self,
        iterate_es_results: mock.MagicMock,
        gene_model: GeneModel,
        ascat_documents: Tuple[AscatDocument, ...],
    ) -> None:
        inputs = self._arrange_input_dataframes((PrimaryAliquot(),), (gene_model,))
        builder = self._arrange_builder((ESFile(),), ascat_documents)

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 0
