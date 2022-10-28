import dataclasses
from typing import Optional, Tuple

import more_itertools
from pyspark import sql


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
class ASCAT:
    _id: Optional[dict] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aliquot_id: Optional[str] = "aliquot-0"
    available_variation_data: Optional[str] = "cnv"
    biotype: Optional[str] = "protein_coding"
    canonical_transcript_id: Optional[str] = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: Optional[str] = "case-0"
    chromosome: Optional[str] = "1"
    cnv_id: Optional[str] = "cnv-0"
    cnv_change: Optional[str] = "Gain"
    consequence_id: Optional[str] = "377b6f05-34e8-51d0-81a6-3a8781032253"
    cytoband: Optional[Tuple[str, ...]] = ("1p36.33",)
    description: Optional[
        str
    ] = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    end_position: Optional[int] = 14409
    entrez_gene: Optional[Tuple[str, ...]] = (
        "100287596",
        "100287102",
        "727856",
        "84771",
    )
    gene_chromosome: Optional[str] = "1"
    gene_end: Optional[int] = 14409
    gene_id: Optional[str] = "CNVGENE"
    gene_level_cn: Optional[bool] = True
    gene_start: Optional[int] = 11869
    gene_strand: Optional[int] = 1
    hgnc: Optional[Tuple[str, ...]] = ("HGNC:37102",)
    is_cancer_gene_census: Optional[str] = "true"
    name: Optional[str] = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: Optional[str] = "GRCh38"
    observation_id: Optional[str] = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
    occurrence_id: Optional[str] = "2d7b55e0-9122-5a30-9a42-81c06fe5183f"
    omim_gene: Optional[Tuple[str, ...]] = ()
    start_position: Optional[int] = 11869
    symbol: Optional[str] = "DDX11L1"
    synonyms: Optional[Tuple[str, ...]] = ()
    transcripts: Optional[Tuple[Transcript, ...]] = (Transcript(),)
    uniprotkb_swissprot: Optional[Tuple[str, ...]] = ()
    variant_caller: Optional[str] = "ASCAT"
    variant_status: Optional[str] = "Tumor Only"


def assert_ascat_transated(result_gene: sql.Row, ascat: ASCAT) -> None:
    result_cnv = more_itertools.one(result_gene.cnv)

    assert result_gene.gene_id == ascat.gene_id
    assert result_gene.biotype == ascat.biotype
    assert result_gene.symbol == ascat.symbol
    assert result_gene.is_cancer_gene_census == ascat.is_cancer_gene_census
    assert result_cnv.cnv_id == ascat.cnv_id
    assert result_cnv.chromosome == ascat.chromosome
    assert result_cnv.cnv_change == ascat.cnv_change
    assert result_cnv.end_position == ascat.end_position
    assert result_cnv.gene_level_cn == ascat.gene_level_cn
    assert result_cnv.ncbi_build == ascat.ncbi_build
    assert result_cnv.start_position == ascat.start_position
