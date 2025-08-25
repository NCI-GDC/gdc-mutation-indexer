import dataclasses

__all__ = ("GeneModel",)


@dataclasses.dataclass(frozen=True)
class GeneModel:
    @dataclasses.dataclass(frozen=True)
    class Transcript:
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

        biotype: str = "processed_transcript"
        cdna_coding_end: int = 0
        cdna_coding_start: int = 0
        coding_region_end: int = 0
        coding_region_start: int = 0
        domains: tuple[Domain, ...] = (Domain(),)
        end: int | None = 14409
        end_exon: int | None = None
        exons: tuple[Exon, ...] = (Exon(),)
        transcript_id: str = "ENST00000456328"
        is_canonical: bool = False
        length: int | None = 1657
        length_amino_acid: int | None = None
        length_cds: int | None = None
        name: str = "DDX11L1-002"
        number_of_exons: int = 6
        seq_exon_end: int | None = None
        seq_exon_start: int | None = None
        start: int | None = 11869
        start_exon: int | None = None
        translation_id: str | None = None

    _gene_id: str = "ENSG00000238009"
    _id: dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"},
    )
    biotype: str = "protein_coding"
    canonical_transcript_id: str = "ENST00000456328"
    chromosome: str = "1"
    cytoband: tuple[str | None, ...] = ("1p36.33",)
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    entrez_gene: tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    gene_end: int = 14409
    gene_start: int = 11869
    gene_strand: int = 1
    hgnc: tuple[str, ...] = ("HGNC:37102",)
    is_cancer_gene_census: str = "true"
    omim_gene: tuple[str, ...] = ()
    uniprotkb_swissprot: tuple[str, ...] = ()
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    symbol: str = "DDX11L1"
    synonyms: tuple[str, ...] = ()
    transcripts: tuple[Transcript, ...] = (Transcript(),)
