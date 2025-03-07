import dataclasses

from tests.unit.data.models.builders import *


@dataclasses.dataclass(frozen=True)
class ASCATMetadata:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "ASCAT3"


@dataclasses.dataclass(frozen=True)
class ASCAT:
    _id: dict | None = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
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
    description: None | (
        str
    ) = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
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
    transcripts: tuple[Transcript, ...] | None = (Transcript(),)
    uniprotkb_swissprot: tuple[str, ...] | None = ()
    variant_caller: str | None = "ASCAT2"
    variant_status: str | None = "Tumor Only"


@dataclasses.dataclass(frozen=True)
class Allele:
    allele_id: str = "03b61092-4545-526e-9b39-fc8005c40af5"


@dataclasses.dataclass(frozen=True)
class MAF:
    _id: dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
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
    transcripts: tuple[Transcript, ...] = (Transcript(),)
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
            normal_bam_uuid: str | None = "604c11f1-ab8b-48a7-909e-982e873e02e5"
            tumor_bam_uuid: str | None = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"

        @dataclasses.dataclass(frozen=True)
        class NormalGenotype:
            match_norm_seq_allele1: str | None = None
            match_norm_seq_allele2: str | None = None

        @dataclasses.dataclass(frozen=True)
        class ReadDepth:
            n_depth: int | None = 38
            t_alt_count: int | None = 5
            t_depth: int | None = 29
            t_ref_count: int | None = 24

        @dataclasses.dataclass(frozen=True)
        class Sample:
            matched_norm_sample_barcode: str | None = "MBCProject_3808_SALIVA_1"
            matched_norm_sample_uuid: None | (
                str
            ) = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
            tumor_sample_barcode: str | None = "MBCProject_3808_T1_WES_1"
            tumor_sample_uuid: str | None = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"

        @dataclasses.dataclass(frozen=True)
        class TumorGenotype:
            tumor_seq_allele1: str | None = "C"
            tumor_seq_allele2: str | None = "A"

        @dataclasses.dataclass(frozen=True)
        class Validation:
            tumor_validation_allele1: str | None = None
            tumor_validation_allele2: str | None = None
            validation_method: str | None = None

        @dataclasses.dataclass(frozen=True)
        class VariantCalling:
            variant_caller: str | None = "muse;varscan2"
            variant_process: str | None = "masked"

        center: str | None = "BI"
        input_bam_file: InputBAMFile | None = InputBAMFile()
        mutation_status: str | None = "Somatic"
        normal_genotype: NormalGenotype | None = NormalGenotype()
        observation_id: str | None = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
        read_depth: ReadDepth | None = ReadDepth()
        sample: Sample | None = Sample()
        tumor_genotype: TumorGenotype | None = TumorGenotype()
        validation: Validation | None = Validation()
        variant_calling: VariantCalling | None = VariantCalling()

    ssm_id: str | None = "ssm-0"
    case_id: str | None = "case-0"
    occurrence_id: str | None = "1746a06f-2052-5fec-8150-8da04c939ee4"
    observation: tuple[Observation, ...] | None = (Observation(),)


@dataclasses.dataclass(frozen=True)
class SSMConsequence:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Transcript:
            @dataclasses.dataclass(frozen=True)
            class Annotation:
                amino_acids: str | None = "A/S"
                ccds: str | None = "CCDS380.1"
                cdna_position: str | None = "1734/13108"
                cds_end: int | None = 12169
                cds_length: int | None = 10464
                cds_position: str | None = "1705/10464"
                cds_start: int | None = 1705
                clin_sig: str | None = "ss"
                codons: str | None = "Gct/Tct"
                dbsnp_rs: str | None = "novel"
                dbsnp_val_status: str | None = None
                domains: str | None = "dfjks;sksk"
                ensp: str | None = "ENSP00000241312"
                existing_variation: str | None = None
                hgvsc: str | None = "c.1705G>T"
                hgvsp: str | None = "p.Ala569Ser"
                hgvsp_short: str | None = "p.A569S"
                polyphen_impact: str | None = "benign"
                polyphen_score: float | None = 0.305
                protein_position: str | None = "569/3487"
                pubmed: str | None = None
                sift_impact: str | None = "tolerated"
                sift_score: float | None = 0.12
                swissprot: str | None = "Q7Z408.146"
                transcript_id: str | None = "ENST00000241312"
                trembl: str | None = "DKD"
                uniparc: str | None = "UPI00004561AB"
                vep_impact: str | None = "MODERATE"

            @dataclasses.dataclass(frozen=True)
            class Gene:
                @dataclasses.dataclass(frozen=True)
                class ExternalDBIds:
                    entrez_gene: tuple[str, ...] | None = (
                        "100287596",
                        "100287102",
                        "727856",
                        "84771",
                    )
                    hgnc: tuple[str, ...] | None = ("HGNC:37102",)
                    omim_gene: tuple[str, ...] | None = ()
                    uniprotkb_swissprot: tuple[str, ...] | None = ()

                biotype: str | None = "transcribed_unprocessed_pseudogene"
                canonical_transcript_id: str | None = "ENST00000456328"
                cytoband: tuple[str, ...] | None = ("1p36.33",)
                external_db_ids: ExternalDBIds | None = ExternalDBIds()
                gene_chromosome: str | None = "1"
                gene_end: int | None = 14409
                gene_id: str | None = "ENSG00000238009"
                gene_start: int | None = 11869
                gene_strand: int | None = 1
                is_cancer_gene_census: str | None = "true"
                symbol: str | None = "CSMD2"
                synonyms: tuple[str, ...] | None = ()

            transcript_id: str | None = "ENST00000241312"
            aa_change: str | None = "R450H"
            aa_end: int | None = 451
            aa_start: int | None = 461
            consequence_type: str | None = "missense_variant;NMD_transcript_variant"
            is_canonical: bool | None = None
            ref_seq_accession: str | None = None
            annotation: Annotation | None = Annotation()
            gene: Gene | None = Gene()

        consequence_id: str | None = "377b6f05-34e8-51d0-81a6-3a8781032253"
        transcript: Transcript | None = Transcript()

    ssm_id: str | None = "ssm-0"
    consequence: tuple[Consequence, ...] | None = (Consequence(),)
    gene_aa_change: tuple[str, ...] | None = ("change",)


@dataclasses.dataclass(frozen=True)
class CNVConsequence:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Gene:
            biotype: str | None = "transcribed_unprocessed_pseudogene"
            gene_id: str | None = "ENSG00000238009"
            is_cancer_gene_census: str | None = "true"
            symbol: str | None = "CSMD2"

        consequence_id: str | None = "cons-0"
        gene: Gene | None = Gene()

    cnv_id: str | None = "cnv-0"
    consequence: tuple[Consequence, ...] | None = (Consequence(),)


@dataclasses.dataclass(frozen=True)
class CNVObservation:
    @dataclasses.dataclass(frozen=True)
    class Observation:
        @dataclasses.dataclass(frozen=True)
        class VariantCalling:
            variant_caller: str | None = "ASCAT"

        copy_number: int | None = 5
        observation_id: str | None = "obs-0"
        sample_ploidy_integer: int | None = 2
        src_file_id: str | None = "file-0"
        variant_calling: VariantCalling | None = VariantCalling()
        variant_status: str | None = "Tumor Only"

    cnv_id: str | None = "cnv-0"
    case_id: str | None = "case-0"
    occurrence_id: str | None = "occ-0"
    observation: tuple[Observation, ...] | None = (Observation(),)


class CIVIC:
    @dataclasses.dataclass(frozen=True)
    class DNA:
        chromosome: str | None = "chr1"
        civic_gene_id: str | None = "dna_gene"
        civic_variant_id: str | None = "dna_variant"
        reference_allele: str | None = "C"
        start_position: int | None = 33772590
        tumor_allele: str | None = "A"

    @dataclasses.dataclass(frozen=True)
    class Protein:
        civic_gene_id: str | None = "dna_gene"
        civic_variant_id: str | None = "dna_variant"
        name: str | None = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
        hgvsp_short: str | None = "p.A569S"


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Demographic:
        age_at_index: int | None = 22
        age_is_obfuscated: str | None = None
        cause_of_death: str | None = "Cancer Related"
        days_to_birth: int | None = -8223
        days_to_death: int | None = 505
        demographic_id: str | None = "a4c9844b-3a9e-4941-8148-077bae8ffcab"
        ethnicity: str | None = "hispanic or latino"
        gender: str | None = "female"
        race: str | None = "white"
        state: str | None = "released"
        submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        vital_status: str | None = "Dead"
        year_of_birth: int | None = 1945
        year_of_death: int | None = 2009

    @dataclasses.dataclass(frozen=True)
    class Diagnosis:
        @dataclasses.dataclass(frozen=True)
        class PathologyDetail:
            anaplasia_present: str | None = "Unknown"
            anaplasia_present_type: str | None = "Unknown"
            bone_marrow_malignant_cells: str | None = "No"
            breslow_thickness: float | None = 2.7
            circumferential_resection_margin: float | None = None
            columnar_mucosa_present: str | None = None
            dysplasia_degree: str | None = None
            dysplasia_type: str | None = None
            greatest_tumor_dimension: float | None = None
            gross_tumor_weight: float | None = None
            largest_extrapelvic_peritoneal_focus: None | (
                str
            ) = "Macroscopic (2cm or less)"
            lymph_node_involved_site: str | None = "Retroperitoneal"
            lymph_node_involvement: str | None = "Positive"
            lymph_nodes_positive: int | None = 1
            lymph_nodes_tested: int | None = 14
            lymphatic_invasion_present: str | None = "No"
            margin_status: str | None = None
            metaplasia_present: str | None = None
            morphologic_architectural_pattern: str | None = "Cohesive"
            non_nodal_regional_disease: str | None = None
            non_nodal_tumor_deposits: str | None = None
            number_proliferating_cells: int | None = None
            pathology_detail_id: str | None = "29d2d011-87ce-4815-b704-250422e26334"
            percent_tumor_invasion: float | None = None
            perineural_invasion_present: str | None = "No"
            peripancreatic_lymph_nodes_positive: str | None = "4 or More"
            peripancreatic_lymph_nodes_tested: int | None = 34
            prostatic_chips_positive_count: float | None = None
            prostatic_chips_total_count: float | None = None
            prostatic_involvement_percent: float | None = None
            state: str | None = "released"
            submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
            transglottic_extension: str | None = None
            tumor_largest_dimension_diameter: float | None = 8.3
            vascular_invasion_present: str | None = "No"
            vascular_invasion_type: str | None = "Intramural"

        @dataclasses.dataclass(frozen=True)
        class Treatment:
            chemo_concurrent_to_radiation: str | None = "Yes"
            days_to_treatment_end: int | None = 397
            days_to_treatment_start: int | None = 214
            initial_disease_status: str | None = "Residual Disease"
            number_of_cycles: int | None = 5
            regimen_or_line_of_therapy: str | None = "FOLFOX"
            state: str | None = "released"
            submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
            therapeutic_agents: str | None = "Gemtuzumab Ozogamicin"
            treatment_anatomic_site: str | None = "Body, total"
            treatment_dose: int | None = 25
            treatment_frequency: str | None = "Once Weekly"
            treatment_id: str | None = "d4ac03f0-cc74-45a4-93e8-2088a94e3d0d"
            treatment_intent_type: str | None = "Adjuvant"
            treatment_or_therapy: str | None = "yes"
            treatment_outcome: str | None = "Unknown"
            treatment_type: str | None = "Stem Cell Transplantation, NOS"

        age_at_diagnosis: int | None = 8223
        ajcc_clinical_m: str | None = "M0"
        ajcc_clinical_n: str | None = "N3"
        ajcc_clinical_stage: str | None = "Stage IIC"
        ajcc_clinical_t: str | None = "T1"
        ajcc_pathologic_m: str | None = "M0"
        ajcc_pathologic_n: str | None = "N3"
        ajcc_pathologic_stage: str | None = "Stage IIIC"
        ajcc_pathologic_t: str | None = "T0"
        ajcc_staging_system_edition: str | None = "7th"
        ann_arbor_b_symptoms: str | None = "No"
        ann_arbor_clinical_stage: str | None = "Stage II"
        ann_arbor_extranodal_involvement: str | None = "Yes"
        ann_arbor_pathologic_stage: str | None = "Stage IV"
        burkitt_lymphoma_clinical_variant: str | None = "Endemic"
        classification_of_tumor: str | None = "primary"
        cog_renal_stage: str | None = "Stage IV"
        days_to_diagnosis: int | None = 0
        days_to_last_follow_up: float | None = 288.0
        days_to_last_known_disease_status: float | None = 292.0
        days_to_recurrence: float | None = 1246.0
        diagnosis_id: str | None = "6a6ce3d0-7f46-416c-980d-e39b639746fe"
        esophageal_columnar_dysplasia_degree: str | None = "Unknown"
        esophageal_columnar_metaplasia_present: str | None = "Unknown"
        figo_stage: str | None = "Stage IC"
        figo_staging_edition_year: str | None = "2009"
        gastric_esophageal_junction_involvement: str | None = "Unknown"
        goblet_cells_columnar_mucosa_present: str | None = "Unknown"
        icd_10_code: str | None = "C92.0"
        igcccg_stage: str | None = "Good Prognosis"
        inss_stage: str | None = "Stage 4"
        international_prognostic_index: str | None = "Low-Intermediate Risk"
        iss_stage: str | None = "II"
        last_known_disease_status: str | None = "not reported"
        laterality: str | None = "Left"
        masaoka_stage: str | None = "Stage IIb"
        metastasis_at_diagnosis: str | None = "No Metastasis"
        metastasis_at_diagnosis_site: str | None = None
        method_of_diagnosis: str | None = "Surgical Resection"
        micropapillary_features: str | None = None
        morphology: str | None = "9861/3"
        pathology_details: tuple[PathologyDetail, ...] | None = (PathologyDetail(),)
        pregnant_at_diagnosis: str | None = "No"
        primary_diagnosis: str | None = "Acute myeloid leukemia, NOS"
        primary_gleason_grade: str | None = "Pattern 4"
        prior_malignancy: str | None = "yes"
        prior_treatment: str | None = "No"
        progression_or_recurrence: str | None = "not reported"
        residual_disease: str | None = "Not Reported"
        secondary_gleason_grade: str | None = "Pattern 3"
        site_of_resection_or_biopsy: str | None = "Not Reported"
        state: str | None = "released"
        submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        synchronous_malignancy: str | None = "Not Reported"
        tissue_or_organ_of_origin: str | None = "Bone marrow"
        treatments: tuple[Treatment, ...] | None = (Treatment(),)
        tumor_grade: str | None = "Not Reported"
        year_of_diagnosis: int | None = 2013

    @dataclasses.dataclass(frozen=True)
    class Exposure:
        alcohol_days_per_week: float | None = 4.0
        alcohol_history: str | None = "Not Reported"
        alcohol_intensity: str | None = "Occasional Drinker"
        asbestos_exposure: str | None = None
        cigarettes_per_day: float | None = 1.095890410958904
        exposure_id: str | None = "a990ee00-9075-5ab8-acd1-02152e1fbbce"
        pack_years_smoked: float | None = 20.0
        radon_exposure: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
        tobacco_smoking_onset_year: int | None = 1973
        tobacco_smoking_quit_year: int | None = 2007
        tobacco_smoking_status: str | None = "Lifelong Non-Smoker"
        years_smoked: float | None = 25.0

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        family_history_id: str | None = "ea9c9292-5f48-405e-8eff-2faf0f4767ab"
        relationship_age_at_diagnosis: float | None = None
        relationship_gender: str | None = "female"
        relationship_primary_diagnosis: str | None = "Melanoma"
        relationship_type: str | None = "First Degree Relative, NOS"
        relative_with_cancer_history: str | None = "unknown"
        state: str | None = "released"
        submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"

    @dataclasses.dataclass(frozen=True)
    class Project:
        @dataclasses.dataclass(frozen=True)
        class Program:
            dbgap_accession_number: str | None = "phs000218"
            name: str | None = "TARGET"
            program_id: str | None = "f1c391e9-8488-55a8-b777-302e786ea11d"

        dbgap_accession_number: str | None = "phs000218"
        disease_type: tuple[str, ...] | None = (
            "Not Applicable",
            "Myeloid Leukemias",
        )
        intended_release_date: str | None = None
        name: str | None = "TARGET"
        primary_site: tuple[str, ...] | None = (
            "Unknown",
            "Hematopoietic and reticuloendothelial systems",
        )
        program: Program | None = Program()
        project_id: str | None = "TARGET-AML"

    @dataclasses.dataclass(frozen=True)
    class Sample:
        preservation_method: str | None = "Unknown"
        sample_type: str | None = "Bone Marrow Normal"
        specimen_type: str | None = "Bone Marrow NOS"
        tissue_type: str | None = "Normal"
        tumor_descriptor: str | None = "Not Applicable"

    @dataclasses.dataclass(frozen=True)
    class TissueSourceSite:
        bcr_id: str | None = "NCH"
        code: str | None = "02"
        name: str | None = "TARGET"
        project: str | None = "Uterine Corpus Endometrial Carcinoma"
        tissue_source_site_id: str | None = "cee96273-f2ef-52a4-8f36-64f19a81eb32"

    case_id: str | None = "case-0"
    consent_type: str | None = "Informed Consent"
    days_to_consent: int | None = -2
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    disease_type: str | None = "['Not Applicable', 'Myeloid Leukemias']"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    index_date: str | None = "Diagnosis"
    lost_to_followup: str | None = "Yes"
    primary_site: None | (
        str
    ) = "['Unknown', 'Hematopoietic and reticuloendothelial systems']"
    project: Project | None = Project()
    samples: tuple[Sample, ...] | None = (Sample(),)
    state: str | None = "released"
    submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    available_variation_data: tuple[str, ...] | None = None
