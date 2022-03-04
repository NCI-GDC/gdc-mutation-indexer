from os import path
from typing import Dict, Iterable, Mapping, Optional, Tuple
from unittest import mock

import attr
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.builders import ascat
from tests.unit import utils


@attr.s(frozen=True)
class AscatDocument:
    did = attr.ib(type=str, default="file-0")
    gene_id = attr.ib(type=str, default="ENSG00000238009")
    gene_name = attr.ib(type=str, default="RP11-34P13.7")
    chromosome = attr.ib(type=str, default="chr1")
    start = attr.ib(type=int, default=0)
    end = attr.ib(type=int, default=100)
    copy_number = attr.ib(type=int, default=33)
    min_copy_number = attr.ib(type=int, default=0)
    max_copy_number = attr.ib(type=int, default=100)


@attr.s(frozen=True)
class ESAliquot:
    aliquot_id = attr.ib(type=str, default="aliquot-0")


@attr.s(frozen=True)
class ESAnalyte:
    aliquots = attr.ib(type=Tuple[ESAliquot, ...], default=(ESAliquot(),))


@attr.s(frozen=True)
class ESPortion:
    analytes = attr.ib(type=Tuple[ESAnalyte, ...], default=(ESAnalyte(),))


@attr.s(frozen=True)
class ESSample:
    portions = attr.ib(type=Tuple[ESPortion, ...], default=(ESPortion(),))


@attr.s(frozen=True)
class ESCase:
    case_id = attr.ib(type=str, default="case-0")
    samples = attr.ib(type=Tuple[ESSample, ...], default=(ESSample(),))


@attr.s(frozen=True)
class ESFile:
    file_id = attr.ib(type=str, default="file-0")
    cases = attr.ib(type=Tuple[ESCase, ...], default=(ESCase(),))


@attr.s(frozen=True)
class Domain:
    description = attr.ib(
        type=str, default="G protein-coupled receptor, rhodopsin-like"
    )
    end = attr.ib(type=int, default=280)
    gff_source = attr.ib(type=str, default="pfam")
    hit_name = attr.ib(type=str, default="PF00001")
    interpro_id = attr.ib(type=str, default="IPR000276")
    start = attr.ib(type=int, default=34)


@attr.s(frozen=True)
class Exon:
    cdna_coding_end = attr.ib(type=int, default=0)
    cdna_coding_start = attr.ib(type=int, default=0)
    cdna_end = attr.ib(type=int, default=359)
    cdna_start = attr.ib(type=int, default=1)
    end = attr.ib(type=int, default=12227)
    end_phase = attr.ib(type=int, default=-1)
    genomic_coding_end = attr.ib(type=int, default=0)
    genomic_coding_stairt = attr.ib(type=int, default=0)
    genomic_coding_start = attr.ib(type=int, default=0)
    start = attr.ib(type=int, default=11869)
    start_phase = attr.ib(type=int, default=-1)


@attr.s(frozen=True)
class Transcript:
    biotype = attr.ib(type=str, default="processed_transcript")
    cdna_coding_end = attr.ib(type=int, default=0)
    cdna_coding_start = attr.ib(type=int, default=0)
    coding_region_end = attr.ib(type=int, default=0)
    coding_region_start = attr.ib(type=int, default=0)
    domains = attr.ib(type=Tuple[Domain, ...], default=(Domain(),))
    end = attr.ib(type=int, default=14409)
    end_exon = attr.ib(type=Optional[int], default=None)
    exons = attr.ib(type=Tuple[Exon, ...], default=(Exon(),))
    transcript_id = attr.ib(type=str, default="ENST00000456328")
    is_canonical = attr.ib(type=bool, default=False)
    length = attr.ib(type=int, default=1657)
    length_amino_acid = attr.ib(type=Optional[int], default=None)
    length_cds = attr.ib(type=Optional[int], default=None)
    name = attr.ib(type=str, default="DDX11L1-002")
    number_of_exons = attr.ib(type=int, default=6)
    seq_exon_end = attr.ib(type=Optional[int], default=None)
    seq_exon_start = attr.ib(type=Optional[int], default=None)
    start = attr.ib(type=int, default=11869)
    start_exon = attr.ib(type=Optional[int], default=None)
    translation_id = attr.ib(type=Optional[str], default=None)


@attr.s(frozen=True)
class GeneModel:
    _gene_id = attr.ib(type=str, default="ENSG00000238009")
    _id = attr.ib(
        type=Dict[str, str],
        default=attr.Factory(lambda: {"$oid": "589c87ca0ef75875ed614a40"}),
    )
    biotype = attr.ib(type=str, default="transcribed_unprocessed_pseudogene")
    canonical_transcript_id = attr.ib(type=str, default="ENST00000456328")
    chromosome = attr.ib(type=str, default="1")
    cytoband = attr.ib(type=Tuple[Optional[str], ...], default=("1p36.33",))
    description = attr.ib(
        type=str,
        default="DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation.",
    )
    entrez_gene = attr.ib(
        type=Tuple[str, ...], default=("100287596", "100287102", "727856", "84771")
    )
    gene_end = attr.ib(type=int, default=14409)
    gene_start = attr.ib(type=int, default=11869)
    gene_strand = attr.ib(type=int, default=1)
    hgnc = attr.ib(type=Tuple[str, ...], default=("HGNC:37102",))
    is_cancer_gene_census = attr.ib(type=str, default="true")
    omim_gene = attr.ib(type=Tuple[str, ...], default=())
    uniprotkb_swissprot = attr.ib(type=Tuple[str, ...], default=())
    name = attr.ib(
        type=str, default="DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    )
    symbol = attr.ib(type=str, default="DDX11L1")
    synonyms = attr.ib(type=Tuple[str, ...], default=())
    transcripts = attr.ib(type=Tuple[Transcript, ...], default=(Transcript(),))


@attr.s(frozen=True)
class PrimaryAliquot:
    entity = attr.ib(type=str, default="file")
    file_id = attr.ib(type=str, default="file-0")
    aliquot_id = attr.ib(type=str, default="aliquot-0")


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


class TestAscatBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self, spark_session: sql.SparkSession, data_dir: str
    ) -> None:
        self.spark_session = spark_session
        self.schema_dir = path.join(data_dir, "schemas", "builders", "ascat")

    def _arrange_doc_dataframe_util(
        self, ascat_document_data: Tuple[AscatDocument, ...]
    ) -> mock.MagicMock:
        ascat_document_df = self.spark_session.createDataFrame(
            ascat_document_data,
            types.StructType(
                [
                    types.StructField("did", types.StringType()),
                    *ascat.RAW_ASCAT_STRUCT.fields,
                ]
            ),
        )

        return _arrange_dataframe_util(ascat_document_df)

    def _arrange_es_dataframe_util(
        self, es_files: Tuple[ESFile, ...]
    ) -> mock.MagicMock:
        es_file_df = self.spark_session.createDataFrame(
            es_files, utils.load_schema(self.schema_dir, "es_file.json")
        )

        return _arrange_dataframe_util(es_file_df)

    def _arrange_builder(
        self, es_files: Tuple[ESFile, ...], ascat_documents: Tuple[AscatDocument, ...]
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
            gene_model, utils.load_schema(self.schema_dir, "input_gene_model.json")
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
        ascat_documents = (AscatDocument(),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)

        assert ascat_df.count() == 1
        assert ascat_df.schema == utils.load_schema(self.schema_dir, "final_ascat.json")

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__input_data_transformed(
        self,
        iterate_es_results: mock.MagicMock,
    ) -> None:
        es_file = ESFile()
        ascat_document = AscatDocument()
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = GeneModel()

        inputs = self._arrange_input_dataframes(primary_aliquots, (gene_model,))
        builder = self._arrange_builder((es_file,), (ascat_document,))

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.biotype == gene_model.biotype
        assert ascat_row.case_id == es_file.cases[0].case_id
        assert ascat_row.end_position == ascat_document.end
        assert ascat_row.gene_chromosome == ascat_document.chromosome
        assert ascat_row.start_position == ascat_document.start
        assert ascat_row.symbol == ascat_document.gene_name

    @pytest.mark.parametrize(
        argnames=("es_files", "ascat_documents", "primary_aliquots", "gene_model"),
        argvalues=(
            (
                (ESFile(file_id="file-1"),),
                (AscatDocument(),),
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
                (AscatDocument(),),
                (PrimaryAliquot(),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                (AscatDocument(did="file-1"),),
                (PrimaryAliquot(),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                (AscatDocument(),),
                (PrimaryAliquot(aliquot_id="aliquot-1"),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                (AscatDocument(),),
                (PrimaryAliquot(file_id="file-1"),),
                (GeneModel(),),
            ),
            (
                (ESFile(),),
                (AscatDocument(),),
                (PrimaryAliquot(),),
                (GeneModel(gene_id="ENSG00000238008"),),
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
        assert ascat_df.schema == utils.load_schema(self.schema_dir, "final_ascat.json")

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__gene_id_stripped(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        es_files = (ESFile(),)
        ascat_documents = (AscatDocument(gene_id="ENSG00000238009.9"),)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())

        assert ascat_row.gene_id == "ENSG00000238009"

    @mock.patch(
        "exports.es_utils.iterate_es_results",
        return_value=_arrange_iterate_es_results_return(("file-0",)),
    )
    def test__build_from_scratch__uuids_generated(
        self, iterate_es_results: mock.MagicMock
    ) -> None:
        es_files = (ESFile(),)
        ascat_document = AscatDocument()
        ascat_documents = (ascat_document,)
        primary_aliquots = (PrimaryAliquot(),)
        gene_model = (GeneModel(),)

        inputs = self._arrange_input_dataframes(primary_aliquots, gene_model)
        builder = self._arrange_builder(es_files, ascat_documents)

        ascat_df = builder.build_from_scratch(**inputs)
        ascat_row = more_itertools.one(ascat_df.collect())
        cnv_id = utils.generate_uuid5(
            ascat_document.chromosome,
            ascat_document.start,
            ascat_document.end,
            ascat_document.copy_number,
        )

        assert ascat_row.cnv_id == cnv_id
        assert ascat_row.consequence_id == utils.generate_uuid5(
            ascat_document.gene_name,
            ascat_document.gene_id,
            gene_model[0].is_cancer_gene_census,
            gene_model[0].biotype,
        )
        assert ascat_row.occurrence_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id
        )
        assert ascat_row.observation_id == utils.generate_uuid5(
            cnv_id, es_files[0].cases[0].case_id, primary_aliquots[0].aliquot_id
        )
