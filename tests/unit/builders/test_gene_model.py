import dataclasses
from typing import Dict, Optional, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders
from mutation_indexer.configuration.builders import viz
from mutation_indexer.constants import build
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class Cytoband:
    ens_gene_id: str = "ENSG00000223972"
    cytoband: Optional[str] = "1p36.33"


@dataclasses.dataclass(frozen=True)
class Census:
    cancer_gene_id: str = "ENSG00000223972"
    is_cancer_gene_census: str = "True"


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
    end: int = 14409
    end_exon: Optional[int] = None
    exons: Tuple[Exon, ...] = (Exon(),)
    id: str = "ENST00000456328"
    is_canonical: bool = False
    length: int = 1657
    length_amino_acid: Optional[int] = None
    length_cds: Optional[int] = None
    name: str = "DDX11L1-002"
    number_of_exons: int = 6
    seq_exon_end: Optional[int] = None
    seq_exon_start: Optional[int] = None
    start: int = 11869
    start_exon: Optional[int] = None
    translation_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class ExternalIDs:
    entrez_gene: Tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    hgnc: Tuple[str, ...] = ("HGNC:37102",)
    omim_gene: Tuple[str, ...] = ()
    uniprotkb_swissprot: Tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class GeneModel:
    _gene_id: str = "ENSG00000223972"
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    biotype: str = "transcribed_unprocessed_pseudogene"
    cancer_gene_id: str = "de652d52-4579-4cd9-beb3-776f3bd6f039"
    canonical_transcript_id: str = "ENST00000456328"
    chromosome: str = "1"
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    end: int = 14409
    ens_gene_id: str = "de652d52-4579-4cd9-beb3-776f3bd6f039"
    external_db_ids: ExternalIDs = ExternalIDs()
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    start: int = 11869
    strand: int = 1
    symbol: str = "DDX11L1"
    synonyms: Tuple[str, ...] = ()
    transcripts: Tuple[Transcript, ...] = (Transcript(),)


@pytest.fixture(scope="class")
def input_schema() -> types.StructType:
    return schemas.Builders.GeneModel.RAW.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


class TestGeneModelBuilder:
    @pytest.fixture(autouse=True)
    def import_fixtures(
        self,
        spark_session: sql.SparkSession,
        input_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.input_schema = input_schema
        self.final_schema = final_schema

    def _arrange_builder(
        self,
        cytobands: Tuple[Cytoband, ...],
        census: Tuple[Census, ...],
        gene_model: Tuple[GeneModel, ...],
    ) -> builders.GeneModelBuilder:
        cytoband_df = self.spark_session.createDataFrame(
            cytobands,  # type: ignore
            "ens_gene_id: string, cytoband: string",
        )
        census_df = self.spark_session.createDataFrame(
            census, ("cancer_gene_id", "is_cancer_gene_census")  # type: ignore
        )
        gene_model_df = self.spark_session.createDataFrame(
            gene_model,  # type: ignore
            self.input_schema,
        )
        dataframes = {
            "cytobands": cytoband_df,
            "census": census_df,
            "gene_model": gene_model_df,
        }

        dataframe_reader = mock.MagicMock()
        dataframe_reader.csv.side_effect = lambda path, **_: dataframes.get(path)
        dataframe_reader.json.side_effect = dataframes.get

        sql_context = mock.MagicMock(
            read=dataframe_reader,
        )

        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")
        config = mock.MagicMock(
            spec=viz.GeneModelBuilder,
            backup=backup,
            is_cached=False,
            citobands_file="cytobands",
            census_file="census",
            gene_model_file="gene_model",
        )

        return builders.GeneModelBuilder(config, sql_context)

    @pytest.mark.parametrize(
        ("cytoband_gene_id", "census_gene_id"),
        (
            ("ENSG00000223972", "ENSG00000223972"),
            ("ENSG00000223971", "ENSG00000223972"),
            ("ENSG00000223972", "ENSG00000223971"),
            ("ENSG00000223971", "ENSG00000223971"),
        ),
        ids=(
            "both_cytoband_and_census_exist",
            "no_cytoband_data_exists",
            "no_census_data_exists",
            "neither_cytoband_or_census_exist",
        ),
    )
    def test__build__joins(self, cytoband_gene_id: str, census_gene_id: str) -> None:
        builder = self._arrange_builder(
            (Cytoband(ens_gene_id=cytoband_gene_id),),
            (Census(cancer_gene_id=census_gene_id),),
            (GeneModel(),),
        )

        result_df = builder.build()

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__input_data_transformed(self) -> None:
        cytoband = Cytoband()
        census = Census()
        gene_model = GeneModel()

        builder = self._arrange_builder(
            (cytoband,),
            (census,),
            (gene_model,),
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row._id.asDict() == gene_model._id
        assert result_row.biotype == gene_model.biotype
        assert result_row.canonical_transcript_id == gene_model.canonical_transcript_id
        assert result_row.chromosome == gene_model.chromosome
        assert result_row.description == gene_model.description
        assert tuple(result_row.entrez_gene) == gene_model.external_db_ids.entrez_gene
        assert result_row.gene_end == gene_model.end
        assert result_row.gene_start == gene_model.start
        assert result_row.gene_strand == gene_model.strand
        assert tuple(result_row.hgnc) == gene_model.external_db_ids.hgnc
        assert result_row.name == gene_model.name
        assert tuple(result_row.omim_gene) == gene_model.external_db_ids.omim_gene
        assert result_row.symbol == gene_model.symbol
        assert tuple(result_row.synonyms) == gene_model.synonyms
        assert (
            tuple(result_row.uniprotkb_swissprot)
            == gene_model.external_db_ids.uniprotkb_swissprot
        )
        assert len(result_row.transcripts) == len(gene_model.transcripts)

        for row_transcript, model_transcript in zip(
            result_row.transcripts, gene_model.transcripts
        ):
            assert row_transcript.transcript_id == model_transcript.id
            assert row_transcript.biotype == model_transcript.biotype
            assert row_transcript.cdna_coding_end == model_transcript.cdna_coding_end
            assert (
                row_transcript.cdna_coding_start == model_transcript.cdna_coding_start
            )
            assert (
                row_transcript.coding_region_end == model_transcript.coding_region_end
            )
            assert (
                row_transcript.coding_region_start
                == model_transcript.coding_region_start
            )
            assert row_transcript.end == model_transcript.end
            assert row_transcript.end == model_transcript.end
            assert row_transcript.end_exon == model_transcript.end_exon
            assert row_transcript.is_canonical == model_transcript.is_canonical
            assert row_transcript.length == model_transcript.length
            assert (
                row_transcript.length_amino_acid == model_transcript.length_amino_acid
            )
            assert row_transcript.length_cds == model_transcript.length_cds
            assert row_transcript.name == model_transcript.name
            assert row_transcript.number_of_exons == model_transcript.number_of_exons
            assert row_transcript.seq_exon_end == model_transcript.seq_exon_end
            assert row_transcript.seq_exon_start == model_transcript.seq_exon_start
            assert row_transcript.start == model_transcript.start
            assert row_transcript.start_exon == model_transcript.start_exon
            assert row_transcript.translation_id == model_transcript.translation_id

            for row_domain, domain in more_itertools.zip_equal(
                row_transcript.domains, model_transcript.domains
            ):
                assert row_domain.description == domain.description
                assert row_domain.end == domain.end
                assert row_domain.gff_source == domain.gff_source
                assert row_domain.hit_name == domain.hit_name
                assert row_domain.interpro_id == domain.interpro_id
                assert row_domain.start == domain.start

            for row_exon, exon in more_itertools.zip_equal(
                row_transcript.exons, model_transcript.exons
            ):
                assert row_exon.cdna_coding_end == exon.cdna_coding_end
                assert row_exon.cdna_coding_start == exon.cdna_coding_start
                assert row_exon.cdna_end == exon.cdna_end
                assert row_exon.cdna_start == exon.cdna_start
                assert row_exon.end == exon.end
                assert row_exon.end_phase == exon.end_phase
                assert row_exon.genomic_coding_end == exon.genomic_coding_end
                assert row_exon.genomic_coding_stairt == exon.genomic_coding_stairt
                assert row_exon.genomic_coding_start == exon.genomic_coding_start
                assert row_exon.start == exon.start
                assert row_exon.start_phase == exon.start_phase

    @pytest.mark.parametrize(("cytobands",), (("8di",), ("123,456",)))
    def test__build__cytobands_split(self, cytobands: str) -> None:
        builder = self._arrange_builder(
            (Cytoband(cytoband=cytobands),), (Census(),), (GeneModel(),)
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cytoband == cytobands.split(",")

    @pytest.mark.parametrize(("cytobands",), (("",), (None,)), ids=("Empty", "None"))
    def test__build__cytobands_null_or_empty(self, cytobands: Optional[str]) -> None:
        builder = self._arrange_builder(
            (Cytoband(cytoband=cytobands),), (Census(),), (GeneModel(),)
        )

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cytoband == [cytobands]

    def test__build__is_cancer_gene_census_lowwered(self) -> None:
        builder = self._arrange_builder((Cytoband(),), (Census(),), (GeneModel(),))

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.is_cancer_gene_census == "true"
