import dataclasses
from collections.abc import Iterable
from typing import Mapping, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.builders import ascat
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


DEFAULT_ASCAT_DOCUMENTS = (
    AscatDocument(),
    AscatDocument(copy_number=30),
    AscatDocument(copy_number=30),
)


def _arrange_dataframe_util(dataframe: sql.DataFrame) -> mock.MagicMock:
    dataframe_util = mock.MagicMock()

    dataframe_util.get_dataframe.return_value = dataframe

    return dataframe_util


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
    return schemas.Viz.Builders.ASCAT.DOCUMENT.load()


@pytest.fixture(scope="class")
def es_file_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FILE.load()


@pytest.fixture(scope="class")
def input_gene_model_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


@pytest.fixture(scope="class")
def final_ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


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
            ascat_document_data,  # type: ignore
            self.input_ascat_schema,
        )

        return _arrange_dataframe_util(ascat_document_df)

    def _arrange_es_dataframe_util(
        self, es_files: Tuple[ESFile, ...]
    ) -> mock.MagicMock:
        es_file_df = self.spark_session.createDataFrame(
            es_files,  # type: ignore
            self.es_file_schema,
        )
        util = mock.MagicMock()
        util.read.return_value = es_file_df

        return util

    def _arrange_builder(
        self,
        es_files: Tuple[ESFile, ...],
        ascat_documents: Tuple[AscatDocument, ...] = DEFAULT_ASCAT_DOCUMENTS,
    ) -> builders.ASCATBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")
        config = mock.MagicMock(
            spec=viz.ASCATBuilder,
            omit_cnv_data=False,
            is_cached=False,
            backup=backup,
            projects=(),
        )
        mock_sql_context = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util(ascat_documents)
        es_dataframe_util = self._arrange_es_dataframe_util(es_files)
        doc_resolver = mock.MagicMock(spec=ascat.DocumentResolver)
        doc_resolver.get_ids.return_value = ("file-0",)

        return builders.ASCATBuilder(
            config,
            mock_sql_context,
            doc_dataframe_util,
            es_dataframe_util,
            doc_resolver,
        )

    def _arrange_input_dataframes(
        self,
        primary_aliquots: Tuple[models.PrimaryAliquot, ...],
        gene_model: Tuple[models.GeneModel, ...],
    ) -> Mapping[str, sql.DataFrame]:
        primary_aliquot_df = self.spark_session.createDataFrame(primary_aliquots)  # type: ignore
        gene_model_df = self.spark_session.createDataFrame(
            gene_model,  # type: ignore
            self.input_gene_model_schema,
        )

        return {
            "primary_aliquot_df": primary_aliquot_df,
            "gene_model_df": gene_model_df,
        }

    def test__build__joins_single_record(self) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (models.GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 1
        assert ascat_df.schema == self.final_ascat_schema

    def test__build__input_data_transformed(self) -> None:
        es_file = ESFile()
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = models.GeneModel()

        inputs = self._arrange_input_dataframes(primary_aliquots, (gene_model,))
        builder = self._arrange_builder((es_file,))

        ascat_df = builder.build(**inputs)
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
                (models.PrimaryAliquot(),),
                (models.GeneModel(),),
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
                (models.PrimaryAliquot(),),
                (models.GeneModel(),),
            ),
            (
                (ESFile(),),
                (
                    AscatDocument(did="file-1"),
                    AscatDocument(did="file-1", copy_number=30),
                    AscatDocument(did="file-1", copy_number=30),
                ),
                (models.PrimaryAliquot(),),
                (models.GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (models.PrimaryAliquot(aliquot_id="aliquot-1"),),
                (models.GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (models.PrimaryAliquot(file_id="file-1"),),
                (models.GeneModel(),),
            ),
            (
                (ESFile(),),
                DEFAULT_ASCAT_DOCUMENTS,
                (models.PrimaryAliquot(),),
                (models.GeneModel(_gene_id="ENSG00000238008"),),
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
    def test__build__failed_joins(
        self,
        es_files: Tuple[ESFile, ...],
        ascat_documents: Tuple[AscatDocument, ...],
        primary_aliquots: Tuple[models.PrimaryAliquot, ...],
        gene_model: Tuple[models.GeneModel, ...],
    ) -> None:
        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0
        assert ascat_df.schema == self.final_ascat_schema

    def test__build__gene_id_stripped(self) -> None:
        es_files = (ESFile(),)
        ascat_documents = (
            AscatDocument(gene_id="ENSG00000238009.9"),
            AscatDocument(copy_number=30),
            AscatDocument(copy_number=30),
        )
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (models.GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build(**inputs)
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
    def test__build__copy_number_maps_to_cnv_change(
        self, copy_numbers: Tuple[int, ...], cnv_change: str
    ) -> None:
        es_files = (ESFile(),)
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
        )
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (models.GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.cnv_change == cnv_change

    @pytest.mark.parametrize(
        "copy_numbers",
        ((30,), (31, 32, 33), (33, 20, 20, 40, 40)),
    )
    def test__build__neutral_copy_numbers_filtered(
        self, copy_numbers: Iterable[int]
    ) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (models.GeneModel(),)
        ascat_documents = tuple(
            AscatDocument(copy_number=copy_number) for copy_number in copy_numbers
        )

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build(**inputs)

        assert ascat_df.count() == 0

    def test__build__uuids_generated(self) -> None:
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = models.GeneModel()

        inputs = self._arrange_input_dataframes(primary_aliquots, (gene_model,))
        builder = self._arrange_builder(es_files)

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
        assert ascat_row.occurrence_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id
        )
        assert ascat_row.observation_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id, primary_aliquots[0].aliquot_id
        )

    def test__build__canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = models.Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = models.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (
            models.GeneModel(transcripts=(other_transcript, canonical_transcript)),
        )

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

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
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (
            models.GeneModel(transcripts=(other_transcript, canonical_transcript)),
        )

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    def test__build__canonical_transcript_lengths_no_canonical_transcript(
        self,
    ) -> None:
        transcript = models.Transcript(length=10, length_cds=3, end=122, start=100)
        es_files = (ESFile(),)
        primary_aliquots = (models.PrimaryAliquot(entity="file"),)
        gene_model = (models.GeneModel(transcripts=(transcript,)),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    @pytest.mark.parametrize(
        ("gene_model", "ascat_documents"),
        (
            (
                models.GeneModel(biotype="transcribed_unprocessed_pseudogene"),
                (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
            ),
            (
                models.GeneModel(chromosome="X"),
                (AscatDocument(copy_number=30), AscatDocument(), AscatDocument()),
            ),
            (
                models.GeneModel(),
                (
                    AscatDocument(copy_number=30, chromosome="X"),
                    AscatDocument(),
                    AscatDocument(),
                ),
            ),
        ),
        ids=("non_protein_coding", "gm_x_chromosome", "ascat_x_chromosome"),
    )
    def test__build__filter_gene_model(
        self, gene_model: models.GeneModel, ascat_documents: Tuple[AscatDocument, ...]
    ) -> None:
        inputs = self._arrange_input_dataframes(
            (models.PrimaryAliquot(entity="file"),), (gene_model,)
        )
        builder = self._arrange_builder((ESFile(),), ascat_documents)

        result_df = builder.build(**inputs)

        assert result_df.count() == 0
