import dataclasses
from typing import Optional

from tests.unit.data.models.builders import *


@dataclasses.dataclass(frozen=True)
class ASCATMetadata:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "ASCAT3"


@dataclasses.dataclass(frozen=True)
class ASCAT:
    _id: Optional[dict] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aliquot_id: Optional[str] = "aliquot-0"
    biotype: Optional[str] = "protein_coding"
    canonical_transcript_id: Optional[str] = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: Optional[str] = "case-0"
    chromosome: Optional[str] = "1"
    cnv_id: Optional[str] = "cnv-0"
    cnv_change: Optional[str] = "Gain"
    cnv_change_5_category: Optional[str] = "Gain"
    consequence_id: Optional[str] = "377b6f05-34e8-51d0-81a6-3a8781032253"
    copy_number: Optional[int] = 5
    cytoband: Optional[tuple[str, ...]] = ("1p36.33",)
    description: Optional[
        str
    ] = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    end_position: Optional[int] = 14409
    entrez_gene: Optional[tuple[str, ...]] = (
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
    hgnc: Optional[tuple[str, ...]] = ("HGNC:37102",)
    is_cancer_gene_census: Optional[str] = "true"
    name: Optional[str] = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: Optional[str] = "GRCh38"
    observation_id: Optional[str] = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
    occurrence_id: Optional[str] = "2d7b55e0-9122-5a30-9a42-81c06fe5183f"
    omim_gene: Optional[tuple[str, ...]] = ()
    sample_ploidy_integer: Optional[int] = 2
    src_file_id: str = "file-0"
    start_position: Optional[int] = 11869
    symbol: Optional[str] = "DDX11L1"
    synonyms: Optional[tuple[str, ...]] = ()
    transcripts: Optional[tuple[Transcript, ...]] = (Transcript(),)
    uniprotkb_swissprot: Optional[tuple[str, ...]] = ()
    variant_caller: Optional[str] = "ASCAT2"
    variant_status: Optional[str] = "Tumor Only"


@dataclasses.dataclass(frozen=True)
class Allele:
    allele_id: str = "03b61092-4545-526e-9b39-fc8005c40af5"


@dataclasses.dataclass(frozen=True)
class MAF:
    _id: dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aa_change: Optional[str] = "R416H"
    aa_end: Optional[int] = 417
    aa_start: Optional[int] = 416
    all_effects: str = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    amino_acids: str = "A/S"
    available_variation_data: tuple[str, ...] = ("ssm",)
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
    cosmic_id: Optional[tuple[str, ...]] = ("COSM1456251",)
    cytoband: tuple[str, ...] = ("1p36.33",)
    dbsnp_rs: str = "novel"
    dbsnp_val_status: Optional[str] = None
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: Optional[str] = "dfjks;sksk"
    empty: None = None
    end_position: int = 33772590
    ensp: str = "ENSP00000241312"
    entrez_gene: tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    existing_variation: Optional[str] = None
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
    omim_gene: tuple[str, ...] = ()
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
    synonyms: tuple[str, ...] = ()
    t_alt_count: int = 5
    t_depth: int = 29
    t_ref_count: int = 24
    transcript_id: str = "ENST00000241312"
    transcripts: tuple[Transcript, ...] = (Transcript(),)
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
    uniprotkb_swissprot: tuple[str, ...] = ()
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


@dataclasses.dataclass(frozen=True)
class SegmentCNV:
    segment_cnv_id: str = "segment_cnv-0"
    occurrence_id: str = "occ-0"
    observation_id: str = "obs-0"
    case_id: str = "case-0"
    aliquot_id: str = "aliquot-0"
    src_file_id: str = "file-0"
    chromosome: str = "chr1"
    variant_caller: str = "AscatNGS"
    variant_status: str = "Tumor Only"
    length: int = 51
    start_position: int = 25
    end_position: int = 75
    cnv_change: str = "Gain"
    cnv_change_5_category: str = "Amplification"
    sample_ploidy_integer: int = 5
    copy_number: int = 10


@dataclasses.dataclass(frozen=True)
class SSMObservation:
    @dataclasses.dataclass(frozen=True)
    class Observation:
        @dataclasses.dataclass(frozen=True)
        class InputBAMFile:
            normal_bam_uuid: Optional[str] = "604c11f1-ab8b-48a7-909e-982e873e02e5"
            tumor_bam_uuid: Optional[str] = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"

        @dataclasses.dataclass(frozen=True)
        class NormalGenotype:
            match_norm_seq_allele1: Optional[str] = None
            match_norm_seq_allele2: Optional[str] = None

        @dataclasses.dataclass(frozen=True)
        class ReadDepth:
            n_depth: Optional[int] = 38
            t_alt_count: Optional[int] = 5
            t_depth: Optional[int] = 29
            t_ref_count: Optional[int] = 24

        @dataclasses.dataclass(frozen=True)
        class Sample:
            matched_norm_sample_barcode: Optional[str] = "MBCProject_3808_SALIVA_1"
            matched_norm_sample_uuid: Optional[
                str
            ] = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
            tumor_sample_barcode: Optional[str] = "MBCProject_3808_T1_WES_1"
            tumor_sample_uuid: Optional[str] = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"

        @dataclasses.dataclass(frozen=True)
        class TumorGenotype:
            tumor_seq_allele1: Optional[str] = "C"
            tumor_seq_allele2: Optional[str] = "A"

        @dataclasses.dataclass(frozen=True)
        class Validation:
            tumor_validation_allele1: Optional[str] = None
            tumor_validation_allele2: Optional[str] = None
            validation_method: Optional[str] = None

        @dataclasses.dataclass(frozen=True)
        class VariantCalling:
            variant_caller: Optional[str] = "muse;varscan2"
            variant_process: Optional[str] = "masked"

        center: Optional[str] = "BI"
        input_bam_file: Optional[InputBAMFile] = InputBAMFile()
        mutation_status: Optional[str] = "Somatic"
        normal_genotype: Optional[NormalGenotype] = NormalGenotype()
        observation_id: Optional[str] = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
        read_depth: Optional[ReadDepth] = ReadDepth()
        sample: Optional[Sample] = Sample()
        tumor_genotype: Optional[TumorGenotype] = TumorGenotype()
        validation: Optional[Validation] = Validation()
        variant_calling: Optional[VariantCalling] = VariantCalling()

    ssm_id: Optional[str] = "ssm-0"
    case_id: Optional[str] = "case-0"
    occurrence_id: Optional[str] = "1746a06f-2052-5fec-8150-8da04c939ee4"
    observation: Optional[tuple[Observation, ...]] = (Observation(),)


@dataclasses.dataclass(frozen=True)
class SSMConsequence:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Transcript:
            @dataclasses.dataclass(frozen=True)
            class Annotation:
                amino_acids: Optional[str] = "A/S"
                ccds: Optional[str] = "CCDS380.1"
                cdna_position: Optional[str] = "1734/13108"
                cds_end: Optional[int] = 12169
                cds_length: Optional[int] = 10464
                cds_position: Optional[str] = "1705/10464"
                cds_start: Optional[int] = 1705
                clin_sig: Optional[str] = "ss"
                codons: Optional[str] = "Gct/Tct"
                dbsnp_rs: Optional[str] = "novel"
                dbsnp_val_status: Optional[str] = None
                domains: Optional[str] = "dfjks;sksk"
                ensp: Optional[str] = "ENSP00000241312"
                existing_variation: Optional[str] = None
                hgvsc: Optional[str] = "c.1705G>T"
                hgvsp: Optional[str] = "p.Ala569Ser"
                hgvsp_short: Optional[str] = "p.A569S"
                polyphen_impact: Optional[str] = "benign"
                polyphen_score: Optional[float] = 0.305
                protein_position: Optional[str] = "569/3487"
                pubmed: Optional[str] = None
                sift_impact: Optional[str] = "tolerated"
                sift_score: Optional[float] = 0.12
                swissprot: Optional[str] = "Q7Z408.146"
                transcript_id: Optional[str] = "ENST00000241312"
                trembl: Optional[str] = "DKD"
                uniparc: Optional[str] = "UPI00004561AB"
                vep_impact: Optional[str] = "MODERATE"

            @dataclasses.dataclass(frozen=True)
            class Gene:
                @dataclasses.dataclass(frozen=True)
                class ExternalDBIds:
                    entrez_gene: Optional[tuple[str, ...]] = (
                        "100287596",
                        "100287102",
                        "727856",
                        "84771",
                    )
                    hgnc: Optional[tuple[str, ...]] = ("HGNC:37102",)
                    omim_gene: Optional[tuple[str, ...]] = ()
                    uniprotkb_swissprot: Optional[tuple[str, ...]] = ()

                biotype: Optional[str] = "transcribed_unprocessed_pseudogene"
                canonical_transcript_id: Optional[str] = "ENST00000456328"
                cytoband: Optional[tuple[str, ...]] = ("1p36.33",)
                external_db_ids: Optional[ExternalDBIds] = ExternalDBIds()
                gene_chromosome: Optional[str] = "1"
                gene_end: Optional[int] = 14409
                gene_id: Optional[str] = "ENSG00000238009"
                gene_start: Optional[int] = 11869
                gene_strand: Optional[int] = 1
                is_cancer_gene_census: Optional[str] = "true"
                symbol: Optional[str] = "CSMD2"
                synonyms: Optional[tuple[str, ...]] = ()

            transcript_id: Optional[str] = "ENST00000241312"
            aa_change: Optional[str] = "R450H"
            aa_end: Optional[int] = 451
            aa_start: Optional[int] = 461
            consequence_type: Optional[str] = "missense_variant;NMD_transcript_variant"
            is_canonical: Optional[bool] = None
            ref_seq_accession: Optional[str] = None
            annotation: Optional[Annotation] = Annotation()
            gene: Optional[Gene] = Gene()

        consequence_id: Optional[str] = "377b6f05-34e8-51d0-81a6-3a8781032253"
        transcript: Optional[Transcript] = Transcript()

    ssm_id: Optional[str] = "ssm-0"
    consequence: Optional[tuple[Consequence, ...]] = (Consequence(),)
    gene_aa_change: Optional[tuple[str, ...]] = ("change",)


@dataclasses.dataclass(frozen=True)
class CNVConsequence:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Gene:
            biotype: Optional[str] = "transcribed_unprocessed_pseudogene"
            gene_id: Optional[str] = "ENSG00000238009"
            is_cancer_gene_census: Optional[str] = "true"
            symbol: Optional[str] = "CSMD2"

        consequence_id: Optional[str] = "cons-0"
        gene: Optional[Gene] = Gene()

    cnv_id: Optional[str] = "cnv-0"
    consequence: Optional[tuple[Consequence, ...]] = (Consequence(),)


@dataclasses.dataclass(frozen=True)
class CNVObservation:
    @dataclasses.dataclass(frozen=True)
    class Observation:
        @dataclasses.dataclass(frozen=True)
        class VariantCalling:
            variant_caller: Optional[str] = "ASCAT"

        copy_number: Optional[int] = 5
        observation_id: Optional[str] = "obs-0"
        sample_ploidy_integer: Optional[int] = 2
        src_file_id: Optional[str] = "file-0"
        variant_calling: Optional[VariantCalling] = VariantCalling()
        variant_status: Optional[str] = "Tumor Only"

    cnv_id: Optional[str] = "cnv-0"
    case_id: Optional[str] = "case-0"
    occurrence_id: Optional[str] = "occ-0"
    observation: Optional[tuple[Observation, ...]] = (Observation(),)


class CIVIC:
    @dataclasses.dataclass(frozen=True)
    class DNA:
        chromosome: Optional[str] = "chr1"
        civic_gene_id: Optional[str] = "dna_gene"
        civic_variant_id: Optional[str] = "dna_variant"
        reference_allele: Optional[str] = "C"
        start_position: Optional[int] = 33772590
        tumor_allele: Optional[str] = "A"

    @dataclasses.dataclass(frozen=True)
    class Protein:
        civic_gene_id: Optional[str] = "dna_gene"
        civic_variant_id: Optional[str] = "dna_variant"
        name: Optional[str] = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
        hgvsp_short: Optional[str] = "p.A569S"


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Demographic:
        age_at_index: Optional[int] = 22
        age_is_obfuscated: Optional[str] = None
        cause_of_death: Optional[str] = "Cancer Related"
        days_to_birth: Optional[int] = -8223
        days_to_death: Optional[int] = 505
        demographic_id: Optional[str] = "a4c9844b-3a9e-4941-8148-077bae8ffcab"
        ethnicity: Optional[str] = "hispanic or latino"
        gender: Optional[str] = "female"
        race: Optional[str] = "white"
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        vital_status: Optional[str] = "Dead"
        year_of_birth: Optional[int] = 1945
        year_of_death: Optional[int] = 2009

    @dataclasses.dataclass(frozen=True)
    class Diagnosis:
        @dataclasses.dataclass(frozen=True)
        class PathologyDetail:
            anaplasia_present: Optional[str] = "Unknown"
            anaplasia_present_type: Optional[str] = "Unknown"
            bone_marrow_malignant_cells: Optional[str] = "No"
            breslow_thickness: Optional[float] = 2.7
            circumferential_resection_margin: Optional[float] = None
            columnar_mucosa_present: Optional[str] = None
            dysplasia_degree: Optional[str] = None
            dysplasia_type: Optional[str] = None
            greatest_tumor_dimension: Optional[float] = None
            gross_tumor_weight: Optional[float] = None
            largest_extrapelvic_peritoneal_focus: Optional[
                str
            ] = "Macroscopic (2cm or less)"
            lymph_node_involved_site: Optional[str] = "Retroperitoneal"
            lymph_node_involvement: Optional[str] = "Positive"
            lymph_nodes_positive: Optional[int] = 1
            lymph_nodes_tested: Optional[int] = 14
            lymphatic_invasion_present: Optional[str] = "No"
            margin_status: Optional[str] = None
            metaplasia_present: Optional[str] = None
            morphologic_architectural_pattern: Optional[str] = "Cohesive"
            non_nodal_regional_disease: Optional[str] = None
            non_nodal_tumor_deposits: Optional[str] = None
            number_proliferating_cells: Optional[int] = None
            pathology_detail_id: Optional[str] = "29d2d011-87ce-4815-b704-250422e26334"
            percent_tumor_invasion: Optional[float] = None
            perineural_invasion_present: Optional[str] = "No"
            peripancreatic_lymph_nodes_positive: Optional[str] = "4 or More"
            peripancreatic_lymph_nodes_tested: Optional[int] = 34
            prostatic_chips_positive_count: Optional[float] = None
            prostatic_chips_total_count: Optional[float] = None
            prostatic_involvement_percent: Optional[float] = None
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
            transglottic_extension: Optional[str] = None
            tumor_largest_dimension_diameter: Optional[float] = 8.3
            vascular_invasion_present: Optional[str] = "No"
            vascular_invasion_type: Optional[str] = "Intramural"

        @dataclasses.dataclass(frozen=True)
        class Treatment:
            chemo_concurrent_to_radiation: Optional[str] = "Yes"
            days_to_treatment_end: Optional[int] = 397
            days_to_treatment_start: Optional[int] = 214
            initial_disease_status: Optional[str] = "Residual Disease"
            number_of_cycles: Optional[int] = 5
            regimen_or_line_of_therapy: Optional[str] = "FOLFOX"
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
            therapeutic_agents: Optional[str] = "Gemtuzumab Ozogamicin"
            treatment_anatomic_site: Optional[str] = "Body, total"
            treatment_dose: Optional[int] = 25
            treatment_frequency: Optional[str] = "Once Weekly"
            treatment_id: Optional[str] = "d4ac03f0-cc74-45a4-93e8-2088a94e3d0d"
            treatment_intent_type: Optional[str] = "Adjuvant"
            treatment_or_therapy: Optional[str] = "yes"
            treatment_outcome: Optional[str] = "Unknown"
            treatment_type: Optional[str] = "Stem Cell Transplantation, NOS"

        age_at_diagnosis: Optional[int] = 8223
        ajcc_clinical_m: Optional[str] = "M0"
        ajcc_clinical_n: Optional[str] = "N3"
        ajcc_clinical_stage: Optional[str] = "Stage IIC"
        ajcc_clinical_t: Optional[str] = "T1"
        ajcc_pathologic_m: Optional[str] = "M0"
        ajcc_pathologic_n: Optional[str] = "N3"
        ajcc_pathologic_stage: Optional[str] = "Stage IIIC"
        ajcc_pathologic_t: Optional[str] = "T0"
        ajcc_staging_system_edition: Optional[str] = "7th"
        ann_arbor_b_symptoms: Optional[str] = "No"
        ann_arbor_clinical_stage: Optional[str] = "Stage II"
        ann_arbor_extranodal_involvement: Optional[str] = "Yes"
        ann_arbor_pathologic_stage: Optional[str] = "Stage IV"
        burkitt_lymphoma_clinical_variant: Optional[str] = "Endemic"
        classification_of_tumor: Optional[str] = "primary"
        cog_renal_stage: Optional[str] = "Stage IV"
        days_to_diagnosis: Optional[int] = 0
        days_to_last_follow_up: Optional[float] = 288.0
        days_to_last_known_disease_status: Optional[float] = 292.0
        days_to_recurrence: Optional[float] = 1246.0
        diagnosis_id: Optional[str] = "6a6ce3d0-7f46-416c-980d-e39b639746fe"
        esophageal_columnar_dysplasia_degree: Optional[str] = "Unknown"
        esophageal_columnar_metaplasia_present: Optional[str] = "Unknown"
        figo_stage: Optional[str] = "Stage IC"
        figo_staging_edition_year: Optional[str] = "2009"
        gastric_esophageal_junction_involvement: Optional[str] = "Unknown"
        goblet_cells_columnar_mucosa_present: Optional[str] = "Unknown"
        icd_10_code: Optional[str] = "C92.0"
        igcccg_stage: Optional[str] = "Good Prognosis"
        inss_stage: Optional[str] = "Stage 4"
        international_prognostic_index: Optional[str] = "Low-Intermediate Risk"
        iss_stage: Optional[str] = "II"
        last_known_disease_status: Optional[str] = "not reported"
        laterality: Optional[str] = "Left"
        masaoka_stage: Optional[str] = "Stage IIb"
        metastasis_at_diagnosis: Optional[str] = "No Metastasis"
        metastasis_at_diagnosis_site: Optional[str] = None
        method_of_diagnosis: Optional[str] = "Surgical Resection"
        micropapillary_features: Optional[str] = None
        morphology: Optional[str] = "9861/3"
        pathology_details: Optional[tuple[PathologyDetail, ...]] = (PathologyDetail(),)
        pregnant_at_diagnosis: Optional[str] = "No"
        primary_diagnosis: Optional[str] = "Acute myeloid leukemia, NOS"
        primary_gleason_grade: Optional[str] = "Pattern 4"
        prior_malignancy: Optional[str] = "yes"
        prior_treatment: Optional[str] = "No"
        progression_or_recurrence: Optional[str] = "not reported"
        residual_disease: Optional[str] = "Not Reported"
        secondary_gleason_grade: Optional[str] = "Pattern 3"
        site_of_resection_or_biopsy: Optional[str] = "Not Reported"
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        synchronous_malignancy: Optional[str] = "Not Reported"
        tissue_or_organ_of_origin: Optional[str] = "Bone marrow"
        treatments: Optional[tuple[Treatment, ...]] = (Treatment(),)
        tumor_grade: Optional[str] = "Not Reported"
        year_of_diagnosis: Optional[int] = 2013

    @dataclasses.dataclass(frozen=True)
    class Exposure:
        alcohol_days_per_week: Optional[float] = 4.0
        alcohol_history: Optional[str] = "Not Reported"
        alcohol_intensity: Optional[str] = "Occasional Drinker"
        asbestos_exposure: Optional[str] = None
        cigarettes_per_day: Optional[float] = 1.095890410958904
        exposure_id: Optional[str] = "a990ee00-9075-5ab8-acd1-02152e1fbbce"
        pack_years_smoked: Optional[float] = 20.0
        radon_exposure: Optional[str] = None
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        tobacco_smoking_onset_year: Optional[int] = 1973
        tobacco_smoking_quit_year: Optional[int] = 2007
        tobacco_smoking_status: Optional[str] = "Lifelong Non-Smoker"
        years_smoked: Optional[float] = 25.0

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        family_history_id: Optional[str] = "ea9c9292-5f48-405e-8eff-2faf0f4767ab"
        relationship_age_at_diagnosis: Optional[float] = None
        relationship_gender: Optional[str] = "female"
        relationship_primary_diagnosis: Optional[str] = "Melanoma"
        relationship_type: Optional[str] = "First Degree Relative, NOS"
        relative_with_cancer_history: Optional[str] = "unknown"
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"

    @dataclasses.dataclass(frozen=True)
    class Project:
        @dataclasses.dataclass(frozen=True)
        class Program:
            dbgap_accession_number: Optional[str] = "phs000218"
            name: Optional[str] = "TARGET"
            program_id: Optional[str] = "f1c391e9-8488-55a8-b777-302e786ea11d"

        dbgap_accession_number: Optional[str] = "phs000218"
        disease_type: Optional[tuple[str, ...]] = (
            "Not Applicable",
            "Myeloid Leukemias",
        )
        intended_release_date: Optional[str] = None
        name: Optional[str] = "TARGET"
        primary_site: Optional[tuple[str, ...]] = (
            "Unknown",
            "Hematopoietic and reticuloendothelial systems",
        )
        program: Optional[Program] = Program()
        project_id: Optional[str] = "TARGET-AML"

    @dataclasses.dataclass(frozen=True)
    class Sample:
        preservation_method: Optional[str] = "Unknown"
        sample_type: Optional[str] = "Bone Marrow Normal"
        specimen_type: Optional[str] = "Bone Marrow NOS"
        tissue_type: Optional[str] = "Normal"
        tumor_descriptor: Optional[str] = "Not Applicable"

    @dataclasses.dataclass(frozen=True)
    class TissueSourceSite:
        bcr_id: Optional[str] = "NCH"
        code: Optional[str] = "02"
        name: Optional[str] = "TARGET"
        project: Optional[str] = "Uterine Corpus Endometrial Carcinoma"
        tissue_source_site_id: Optional[str] = "cee96273-f2ef-52a4-8f36-64f19a81eb32"

    case_id: Optional[str] = "case-0"
    consent_type: Optional[str] = "Informed Consent"
    days_to_consent: Optional[int] = -2
    demographic: Optional[Demographic] = Demographic()
    diagnoses: Optional[tuple[Diagnosis, ...]] = (Diagnosis(),)
    disease_type: Optional[str] = "['Not Applicable', 'Myeloid Leukemias']"
    exposures: Optional[tuple[Exposure, ...]] = (Exposure(),)
    family_histories: Optional[tuple[FamilyHistory, ...]] = (FamilyHistory(),)
    index_date: Optional[str] = "Diagnosis"
    lost_to_followup: Optional[str] = "Yes"
    primary_site: Optional[
        str
    ] = "['Unknown', 'Hematopoietic and reticuloendothelial systems']"
    project: Optional[Project] = Project()
    samples: Optional[tuple[Sample, ...]] = (Sample(),)
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
    tissue_source_site: Optional[TissueSourceSite] = TissueSourceSite()
    available_variation_data: Optional[tuple[str, ...]] = None
