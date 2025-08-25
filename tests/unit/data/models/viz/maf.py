import dataclasses
from typing import ClassVar

from tests.unit.data.models import builders


@dataclasses.dataclass(frozen=True)
class MAF:
    Transcript: ClassVar[type[builders.GeneModel.Transcript]] = builders.GeneModel.Transcript

    @dataclasses.dataclass(frozen=True)
    class Allele:
        allele_id: str = "03b61092-4545-526e-9b39-fc8005c40af5"

    _id: dict[str, str] = dataclasses.field(default_factory=lambda: {"$oid": "ye-old-id"})
    aa_change: str | None = "R416H"
    aa_end: int | None = 417
    aa_start: int | None = 416
    all_effects: str = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    amino_acids: str = "A/S"
    available_variation_data: tuple[str, ...] = ("ssm",)
    biotype: str = "transcribed_unprocessed_pseudogene"
    canonical_transcript_id: str = "ENST00000456328"
    canonical_transcript_length: int | None = None
    canonical_transcript_length_cds: int | None = None
    canonical_transcript_length_genomic: int | None = None
    case_id: str = "case-0"
    ccds: str = "CCDS380.1"
    cdna_position: str = "1734/13108"
    cds_end: int = 12169
    cds_length: int = 10464
    cds_position: str = "1705/10464"
    cds_start: int = 1705
    center: str = "BI"
    chromosome: str = "chr1"
    clin_sig: str | None = "ss"
    codons: str = "Gct/Tct"
    consequence_type: str = "missense_variant;NMD_transcript_variant"
    cosmic_id: tuple[str, ...] | None = ("COSM1456251",)
    cytoband: tuple[str, ...] = ("1p36.33",)
    dbsnp_rs: str = "novel"
    dbsnp_val_status: str | None = None
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: str | None = "dfjks;sksk"
    empty: None = None
    end_position: int = 33772590
    ensp: str = "ENSP00000241312"
    entrez_gene: tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    existing_variation: str | None = None
    gene_chromosome: str = "1"
    gene_end: int = 14409
    gene_id: str = "ENSG00000238009"
    gene_start: int = 11869
    gene_strand: int = 1
    genomic_dna_change: str = "chr1:g.33772590C>A"
    hgnc: tuple[str, ...] = ("HGNC:37102",)
    hgvsc: str = "c.1705G>T"
    hgvsp: str = "p.Ala569Ser"
    hgvsp_short: str = "p.A569S"
    is_cancer_gene_census: str = "true"
    is_canonical: bool | None = None
    match_norm_seq_allele1: str | None = None
    match_norm_seq_allele2: str | None = None
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
    omim_gene: tuple[str, ...] = ()
    polyphen_impact: str = "benign"
    polyphen_score: float = 0.305
    protein_position: str = "569/3487"
    pubmed: str | None = None
    ref_seq_accession: str | None = None
    reference_allele: str = "C"
    sift_impact: str = "tolerated"
    sift_score: float = 0.12
    ssm_id: str = "ssm-0"
    start_position: int = 33772590
    swissprot: str = "Q7Z408.146"
    symbol: str = "CSMD2"
    synonyms: tuple[str, ...] = ()
    t_alt_count: int = 5
    t_depth: int = 29
    t_ref_count: int = 24
    transcript_id: str = "ENST00000241312"
    transcripts: tuple[builders.GeneModel.Transcript, ...] = (Transcript(),)
    trembl: str | None = "DKD"
    tumor_allele: str = "A"
    tumor_bam_uuid: str = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"
    tumor_sample_barcode: str = "MBCProject_3808_T1_WES_1"
    tumor_sample_uuid: str = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    tumor_seq_allele1: str = "C"
    tumor_seq_allele2: str = "A"
    tumor_validation_allele1: str | None = None
    tumor_validation_allele2: str | None = None
    uniparc: str = "UPI00004561AB"
    uniprotkb_swissprot: tuple[str, ...] = ()
    validation_method: str | None = None
    variant_caller: str = "muse;varscan2"
    variant_process: str = "masked"
    variant_type: str = "SNP"
    vep_impact: str = "MODERATE"
    civic_gene_id: str = "1"
    civic_variant_id: str = "3"
