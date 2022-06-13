from os import path
from typing import Dict, Optional, Tuple
from unittest import mock

import attr
import more_itertools
import pytest
from pyspark import sql

from mutation_indexer.driver import builders
from tests.unit import utils


@attr.s(frozen=True)
class Cytoband:
    ens_gene_id = attr.ib(type=str, default="ENSG00000223972")
    cytoband = attr.ib(type=Optional[str], default="1p36.33")


@attr.s(frozen=True)
class Census:
    cancer_gene_id = attr.ib(type=str, default="ENSG00000223972")
    is_cancer_gene_census = attr.ib(type=str, default="True")


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
    id = attr.ib(type=str, default="ENST00000456328")
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
class ExternalIDs:
    entrez_gene = attr.ib(
        type=Tuple[str, ...], default=("100287596", "100287102", "727856", "84771")
    )
    hgnc = attr.ib(type=Tuple[str, ...], default=("HGNC:37102",))
    omim_gene = attr.ib(type=Tuple[str, ...], default=())
    uniprotkb_swissprot = attr.ib(type=Tuple[str, ...], default=())


@attr.s(frozen=True)
class GeneModel:
    _gene_id = attr.ib(type=str, default="ENSG00000223972")
    _id = attr.ib(
        type=Dict[str, str],
        default=attr.Factory(lambda: {"$oid": "589c87ca0ef75875ed614a40"}),
    )
    biotype = attr.ib(type=str, default="transcribed_unprocessed_pseudogene")
    cancer_gene_id = attr.ib(type=str, default="de652d52-4579-4cd9-beb3-776f3bd6f039")
    canonical_transcript_id = attr.ib(type=str, default="ENST00000456328")
    chromosome = attr.ib(type=str, default="1")
    description = attr.ib(
        type=str,
        default="DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation.",
    )
    end = attr.ib(type=int, default=14409)
    ens_gene_id = attr.ib(type=str, default="de652d52-4579-4cd9-beb3-776f3bd6f039")
    external_db_ids = attr.ib(type=ExternalIDs, default=ExternalIDs())
    name = attr.ib(
        type=str, default="DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    )
    start = attr.ib(type=int, default=11869)
    strand = attr.ib(type=int, default=1)
    symbol = attr.ib(type=str, default="DDX11L1")
    synonyms = attr.ib(type=Tuple[str, ...], default=())
    transcripts = attr.ib(type=Tuple[Transcript, ...], default=(Transcript(),))


class TestGeneModelBuilder:
    @pytest.fixture(autouse=True)
    def import_fixtures(self, spark_session: sql.SparkSession, data_dir: str) -> None:
        self.spark_session = spark_session
        self.schema_dir = path.join(data_dir, "schemas/builders/gene_model")

    def _arrange_builder(
        self,
        cytobands: Tuple[Cytoband, ...],
        census: Tuple[Census, ...],
        gene_model: Tuple[GeneModel, ...],
    ) -> builders.GeneModelBuilder:
        cytoband_df = self.spark_session.createDataFrame(
            cytobands, "ens_gene_id: string, cytoband: string"
        )
        census_df = self.spark_session.createDataFrame(
            census, ("cancer_gene_id", "is_cancer_gene_census")
        )
        gene_model_df = self.spark_session.createDataFrame(
            gene_model, utils.load_schema(self.schema_dir, "raw_gene_model.json")
        )
        dataframes = {
            "cytobands": cytoband_df,
            "census": census_df,
            "gene_model": gene_model_df,
        }

        dataframe_reader = mock.MagicMock()
        dataframe_reader.option.return_value = dataframe_reader
        dataframe_reader.format.return_value = dataframe_reader
        dataframe_reader.load.side_effect = dataframes.get
        dataframe_reader.json.side_effect = dataframes.get

        sql_context = mock.MagicMock(
            read=dataframe_reader,
        )

        config = mock.MagicMock(
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
    def test__build_from_scratch__joins(
        self, cytoband_gene_id: str, census_gene_id: str
    ) -> None:
        builder = self._arrange_builder(
            (Cytoband(ens_gene_id=cytoband_gene_id),),
            (Census(cancer_gene_id=census_gene_id),),
            (GeneModel(),),
        )

        result_df = builder.build_from_scratch()

        assert result_df.count() == 1
        assert result_df.schema == utils.load_schema(
            self.schema_dir, "final_gene_model.json"
        )

    def test__build_from_scratch__input_data_transformed(self) -> None:
        cytoband = Cytoband()
        census = Census()
        gene_model = GeneModel()

        builder = self._arrange_builder(
            (cytoband,),
            (census,),
            (gene_model,),
        )

        result_df = builder.build_from_scratch()
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
            row_transcript = row_transcript.asDict(recursive=True)
            model_transcript = attr.asdict(model_transcript, recurse=True)

            row_transcript.pop("transcript_id", None)
            model_transcript.pop("id", None)

            assert row_transcript == model_transcript

    def test__build_from_scratch__trascript_id_renamed(self):
        transcript = Transcript()

        builder = self._arrange_builder(
            (Cytoband(),),
            (Census(),),
            (GeneModel(transcripts=(transcript,)),),
        )

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(result_df.collect())
        result_transcript = more_itertools.one(result_row.transcripts)

        assert result_transcript.transcript_id == transcript.id

    @pytest.mark.parametrize(("cytobands",), (("8di",), ("123,456",)))
    def test__build_from_scratch__cytobands_split(self, cytobands: str) -> None:
        builder = self._arrange_builder(
            (Cytoband(cytoband=cytobands),), (Census(),), (GeneModel(),)
        )

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cytoband == cytobands.split(",")

    @pytest.mark.parametrize(("cytobands",), (("",), (None,)), ids=("Empty", "None"))
    def test__build_from_scratch__cytobands_null_or_empty(
        self, cytobands: Optional[str]
    ) -> None:
        builder = self._arrange_builder(
            (Cytoband(cytoband=cytobands),), (Census(),), (GeneModel(),)
        )

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cytoband == [cytobands]

    def test__build_from_scratch__is_cancer_gene_census_lowwered(self) -> None:
        builder = self._arrange_builder((Cytoband(),), (Census(),), (GeneModel(),))

        result_df = builder.build_from_scratch()
        result_row = more_itertools.one(result_df.collect())

        assert result_row.is_cancer_gene_census == "true"
