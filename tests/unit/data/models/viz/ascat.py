import dataclasses
from typing import ClassVar

from tests.unit.data.models import builders


@dataclasses.dataclass(frozen=True)
class ASCAT:
    Transcript: ClassVar[type[builders.GeneModel.Transcript]] = builders.GeneModel.Transcript

    _id: dict | None = dataclasses.field(default_factory=lambda: {"$oid": "ye-old-id"})
    aliquot_id: str | None = "aliquot-0"
    biotype: str | None = "protein_coding"
    canonical_transcript_id: str | None = "ENST00000456328"
    canonical_transcript_length: int | None = None
    canonical_transcript_length_cds: int | None = None
    canonical_transcript_length_genomic: int | None = None
    case_id: str | None = "case-0"
    chromosome: str | None = "1"
    cnv_id: str | None = "cnv-0"
    cnv_change: str | None = "Gain"
    cnv_change_5_category: str | None = "Gain"
    consequence_id: str | None = "377b6f05-34e8-51d0-81a6-3a8781032253"
    copy_number: int | None = 5
    cytoband: tuple[str, ...] | None = ("1p36.33",)
    description: None | (str) = (
        "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    )
    end_position: int | None = 14409
    entrez_gene: tuple[str, ...] | None = (
        "100287596",
        "100287102",
        "727856",
        "84771",
    )
    gene_chromosome: str | None = "1"
    gene_end: int | None = 14409
    gene_id: str | None = "ENSG00000238009"
    gene_level_cn: bool | None = True
    gene_start: int | None = 11869
    gene_strand: int | None = 1
    hgnc: tuple[str, ...] | None = ("HGNC:37102",)
    is_cancer_gene_census: str | None = "true"
    name: str | None = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: str | None = "GRCh38"
    observation_id: str | None = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
    occurrence_id: str | None = "2d7b55e0-9122-5a30-9a42-81c06fe5183f"
    omim_gene: tuple[str, ...] | None = ()
    sample_ploidy_integer: int | None = 2
    src_file_id: str = "file-0"
    start_position: int | None = 11869
    symbol: str | None = "DDX11L1"
    synonyms: tuple[str, ...] | None = ()
    transcripts: tuple[builders.GeneModel.Transcript, ...] | None = (Transcript(),)
    uniprotkb_swissprot: tuple[str, ...] | None = ()
    variant_caller: str | None = "ASCAT2"
    variant_status: str | None = "Tumor Only"
