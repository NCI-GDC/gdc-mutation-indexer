import dataclasses
from typing import Dict, Optional, Tuple

__all__ = ("Domain", "Exon", "GeneModel", "Transcript")


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
    is_cancer_gene_census: bool = True
    omim_gene: Tuple[str, ...] = ()
    uniprotkb_swissprot: Tuple[str, ...] = ()
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    symbol: str = "DDX11L1"
    synonyms: Tuple[str, ...] = ()
    transcripts: Tuple[Transcript, ...] = (Transcript(),)
