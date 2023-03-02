import dataclasses
from typing import Dict, Optional, Tuple

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
    end: int = 14409
    end_exon: Optional[int] = None
    exons: Tuple[Exon, ...] = (Exon(),)
    transcript_id: str = "ENST00000456328"
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
class GeneModel:
    _gene_id: str = "ENSG00000121904"
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    biotype: str = "transcribed_unprocessed_pseudogene"
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


def assert_domains_equal(result_domain: sql.Row, domain: Domain) -> None:
    assert result_domain.description == domain.description
    assert result_domain.end == domain.end
    assert result_domain.gff_source == domain.gff_source
    assert result_domain.hit_name == domain.hit_name
    assert result_domain.interpro_id == domain.interpro_id
    assert result_domain.start == domain.start


def assert_exons_equal(result_exon: sql.Row, exon: Exon) -> None:
    assert result_exon.cdna_coding_end == exon.cdna_coding_end
    assert result_exon.cdna_coding_start == exon.cdna_coding_start
    assert result_exon.cdna_end == exon.cdna_end
    assert result_exon.cdna_start == exon.cdna_start
    assert result_exon.end == exon.end
    assert result_exon.end_phase == exon.end_phase
    assert result_exon.genomic_coding_end == exon.genomic_coding_end
    assert result_exon.genomic_coding_stairt == exon.genomic_coding_stairt
    assert result_exon.genomic_coding_start == exon.genomic_coding_start
    assert result_exon.start == exon.start
    assert result_exon.start_phase == exon.start_phase


def assert_transcripts_equal(
    result_transcript: sql.Row, transcript: Transcript
) -> None:
    assert result_transcript.biotype == transcript.biotype
    assert result_transcript.cdna_coding_end == transcript.cdna_coding_end
    assert result_transcript.cdna_coding_start == transcript.cdna_coding_start
    assert result_transcript.coding_region_end == transcript.coding_region_end
    assert result_transcript.coding_region_start == transcript.coding_region_start
    assert result_transcript.end == transcript.end
    assert result_transcript.end_exon == transcript.end_exon
    assert result_transcript.transcript_id == transcript.transcript_id
    assert result_transcript.is_canonical == transcript.is_canonical
    assert result_transcript.length == transcript.length
    assert result_transcript.length_amino_acid == transcript.length_amino_acid
    assert result_transcript.length_cds == transcript.length_cds
    assert result_transcript.name == transcript.name
    assert result_transcript.number_of_exons == transcript.number_of_exons
    assert result_transcript.seq_exon_end == transcript.seq_exon_end
    assert result_transcript.seq_exon_start == transcript.seq_exon_start
    assert result_transcript.start == transcript.start
    assert result_transcript.start_exon == transcript.start_exon
    assert result_transcript.translation_id == transcript.translation_id

    for result_domain, domain in more_itertools.zip_equal(
        result_transcript.domains, transcript.domains
    ):
        assert_domains_equal(result_domain, domain)

    for result_exon, exon in more_itertools.zip_equal(
        result_transcript.exons, transcript.exons
    ):
        assert_exons_equal(result_exon, exon)

