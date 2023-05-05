import dataclasses
from typing import Dict, Optional, Tuple

from tests.unit.data.models.builders import *


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
    gene_id: Optional[str] = "ENSG00000238009"
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


@dataclasses.dataclass(frozen=True)
class Allele:
    allele_id: str = "03b61092-4545-526e-9b39-fc8005c40af5"


@dataclasses.dataclass(frozen=True)
class MAF:
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aa_change = None
    aa_end = None
    aa_start = None
    all_effects: str = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    amino_acids: str = "A/S"
    available_variation_data: Tuple[str, ...] = ("ssm",)
    biotype: str = "transcribed_unprocessed_pseudogene"
    canonical_transcript_id: str = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: str = "case-0"
    ccds: str = "CCDS380.1"
    cdna_position: str = "1734/13108"
    cds_end: int = 12169
    cds_length: int = 10464
    cds_position: str = "1705/10464"
    cds_start: int = 1705
    center: str = "BI"
    chromosome: str = "chr1"
    clin_sig: Optional[str] = "ss"
    codons: str = "Gct/Tct"
    consequence_type: str = "missense_variant;NMD_transcript_variant"
    cosmic_id: Optional[str] = None
    cytoband: Tuple[str, ...] = ("1p36.33",)
    dbsnp_rs: str = "novel"
    dbsnp_val_status: Optional[str] = None
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: Optional[str] = "dfjks;sksk"
    empty: None = None
    end_position: int = 33772590
    ensp: str = "ENSP00000241312"
    entrez_gene: Tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    existing_variation: Optional[str] = None
    gene_chromosome: str = "1"
    gene_end: int = 14409
    gene_id: str = "ENSG00000238009"
    gene_start: int = 11869
    gene_strand: int = 1
    genomic_dna_change: str = "chr1:g.33772590C>A"
    hgnc: Tuple[str, ...] = ("HGNC:37102",)
    hgvsc: str = "c.1705G>T"
    hgvsp: str = "p.Ala569Ser"
    hgvsp_short: str = "p.A569S"
    is_cancer_gene_census: str = "true"
    is_canonical: Optional[bool] = None
    match_norm_seq_allele1: Optional[str] = None
    match_norm_seq_allele2: Optional[str] = None
    matched_norm_sample_barcode: str = "MBCProject_3808_SALIVA_1"
    matched_norm_sample_uuid: str = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
    mutation_status: str = "Somatic"
    mutation_subtype: str = "Single base substitution"
    mutation_type: str = "Simple Somatic Mutation"
    n_depth: int = 38
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: str = "GRCh38"
    normal_bam_uuid: str = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    normal_genotype: Allele = Allele()
    occurrence_id: str = "1746a06f-2052-5fec-8150-8da04c939ee4"
    omim_gene: Tuple[str, ...] = ()
    polyphen_impact: str = "benign"
    polyphen_score: float = 0.305
    protein_position: str = "569/3487"
    pubmed: Optional[str] = None
    ref_seq_accession: Optional[str] = None
    reference_allele: str = "C"
    sift_impact: str = "tolerated"
    sift_score: float = 0.12
    ssm_id: str = "ssm-0"
    start_position: int = 33772590
    swissprot: str = "Q7Z408.146"
    symbol: str = "CSMD2"
    synonyms: Tuple[str, ...] = ()
    t_alt_count: int = 5
    t_depth: int = 29
    t_ref_count: int = 24
    transcript_id: str = "ENST00000241312"
    transcripts: Tuple[Transcript, ...] = (Transcript(),)
    trembl: Optional[str] = "DKD"
    tumor_allele: str = "A"
    tumor_bam_uuid: str = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"
    tumor_sample_barcode: str = "MBCProject_3808_T1_WES_1"
    tumor_sample_uuid: str = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    tumor_seq_allele1: str = "C"
    tumor_seq_allele2: str = "A"
    tumor_validation_allele1: Optional[str] = None
    tumor_validation_allele2: Optional[str] = None
    uniparc: str = "UPI00004561AB"
    uniprotkb_swissprot: Tuple[str, ...] = ()
    validation_method: Optional[str] = None
    variant_caller: str = "muse;varscan2"
    variant_process: str = "masked"
    variant_type: str = "SNP"
    vep_impact: str = "MODERATE"
    civic_gene_id: str = "1"
    civic_variant_id: str = "3"

@dataclasses.dataclass(frozen=True)
class MAFMetadata:
    case_id: str = "case-0"
    data_type: str = "MAF"
    file_id: str = "f0"
    workflow: str = "MAF Workflow"


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    entity: str = "case"
    entity_id: str = "case-0"
    experimental_strategy: str = "WXS"
    file_id: str = "file-0"
