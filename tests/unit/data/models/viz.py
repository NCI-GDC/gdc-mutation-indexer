import dataclasses

from tests.unit.data.models.builders import Domain, Exon, GeneModel, Transcript

__all__ = (
    "ASCAT",
    "ASCATMetadata",
    "Allele",
    "Case",
    "CIVIC",
    "Domain",
    "Exon",
    "GeneModel",
    "MAF",
    "MAFMetadata",
    "PrimaryAliquot",
    "SegmentCNV",
    "SegmentCNVMetadata",
    "SSMConsequence",
    "SSMObservation",
    "CNVConsequence",
    "CNVObservation",
    "Transcript",
)


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
class SegmentCNVMetadata:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "AscatNGS"
    analysis_id: str = "analysis-0"


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
            matched_norm_sample_uuid: None | (str) = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
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
        age_at_index: int | None = 85
        age_is_obfuscated: str | None = "True"
        cause_of_death: str | None = "Cancer Related"
        cause_of_death_source: str | None = "Medical Record"
        country_of_birth: str | None = "United Kingdom"
        country_of_residence_at_enrollment: str | None = "United States"
        days_to_birth: int | None = -31232
        days_to_death: int | None = 1324
        demographic_id: str | None = "demographic-0"
        education_level: str | None = None
        ethnicity: str | None = "not hispanic or latino"
        gender: str | None = "female"
        marital_status: str | None = None
        occupation_duration_years: int | None = 23
        population_group: str | None = "Ashkenazi Jew"
        race: str | None = "white"
        sex_at_birth: str | None = "female"
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        vital_status: str | None = "Alive"
        year_of_birth: int | None = 1948
        year_of_death: int | None = 2016

    @dataclasses.dataclass(frozen=True)
    class Diagnosis:
        @dataclasses.dataclass(frozen=True)
        class PathologyDetail:
            additional_pathology_findings: str | None = "Extravascular Matrix Loops"
            anaplasia_present: str | None = "No"
            anaplasia_present_type: str | None = "Unknown"
            bone_marrow_malignant_cells: str | None = "No"
            breslow_thickness: float | None = 3.0
            breslow_thickness_category: str | None = None
            circumferential_resection_margin: float | None = 6.0
            columnar_mucosa_present: str | None = None
            consistent_pathology_review: str | None = "Not Reported"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            days_to_pathology_detail: int | None = None
            dysplasia_degree: str | None = None
            dysplasia_type: str | None = None
            epithelioid_cell_percent: float | None = None
            epithelioid_cell_percent_range: str | None = ">90%"
            extracapsular_extension: str | None = "Extensive"
            extracapsular_extension_present: str | None = "Yes"
            extranodal_extension: str | None = "Gross Extension"
            extraocular_nodule_size: str | None = "<=5mm"
            extrascleral_extension: str | None = None
            extrascleral_extension_present: str | None = "No"
            extrathyroid_extension: str | None = "None"
            greatest_tumor_dimension: float | None = 3.0
            gross_tumor_weight: float | None = 300.0
            histologic_progression_type: str | None = None
            intratubular_germ_cell_neoplasia_present: str | None = "Yes"
            largest_extrapelvic_peritoneal_focus: str | None = "Macroscopic (greater than 2cm)"
            lymph_node_dissection_method: str | None = "Functional (Limited) Neck Dissection"
            lymph_node_dissection_site: str | None = "Retroperitoneal"
            lymph_node_involved_site: str | None = "Pelvis, NOS"
            lymph_node_involvement: str | None = "Positive"
            lymph_nodes_positive: int | None = 1
            lymph_nodes_removed: str | None = "No"
            lymph_nodes_tested: int | None = 7
            lymphatic_invasion_present: str | None = "Yes"
            margin_status: str | None = "Uninvolved"
            measurement_type: str | None = "Pathologic"
            measurement_unit: str | None = "Centimeters"
            metaplasia_present: str | None = None
            micrometastasis_present: str | None = None
            morphologic_architectural_pattern: str | None = "Cohesive"
            necrosis_percent: float | None = 10.0
            necrosis_present: str | None = "Yes"
            non_nodal_regional_disease: str | None = None
            non_nodal_tumor_deposits: str | None = "Yes"
            number_proliferating_cells: int | None = None
            pathology_detail_id: str | None = "pathology-detail-0"
            percent_tumor_invasion: float | None = 8.3
            percent_tumor_nuclei: float | None = 95.0
            perineural_invasion_present: str | None = "No"
            peripancreatic_lymph_nodes_positive: str | None = "1-3"
            peripancreatic_lymph_nodes_tested: int | None = 77
            prcc_type: str | None = "Unknown"
            prostatic_chips_positive_count: float | None = None
            prostatic_chips_total_count: float | None = None
            prostatic_involvement_percent: float | None = None
            residual_tumor: str | None = None
            residual_tumor_measurement: str | None = "1-10 mm"
            rhabdoid_percent: float | None = None
            rhabdoid_present: str | None = "No"
            sarcomatoid_percent: float | None = 90.0
            sarcomatoid_present: str | None = "No"
            size_extraocular_nodule: float | None = None
            spindle_cell_percent: float | None = None
            spindle_cell_percent_range: str | None = ">90%"
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            timepoint_category: str | None = "Last Contact"
            transglottic_extension: str | None = "Present"
            tumor_basal_diameter: float | None = 12.0
            tumor_burden: float | None = 1.5
            tumor_depth_descriptor: str | None = "Deep"
            tumor_depth_measurement: float | None = 6.0
            tumor_infiltrating_lymphocytes: str | None = "Unknown"
            tumor_infiltrating_macrophages: str | None = "Unknown"
            tumor_largest_dimension_diameter: float | None = 3.0
            tumor_length_measurement: float | None = 1.3
            tumor_level_prostate: tuple[str, ...] | None = ("Apex", "Middle", "Base")
            tumor_shape: str | None = "Dome"
            tumor_thickness: float | None = None
            tumor_width_measurement: float | None = 1.2
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
            vascular_invasion_present: str | None = "Yes"
            vascular_invasion_type: str | None = "No Vascular Invasion"
            zone_of_origin_prostate: str | None = "Unknown zone"

        @dataclasses.dataclass(frozen=True)
        class Treatment:
            chemo_concurrent_to_radiation: str | None = "Yes"
            clinical_trial_indicator: str | None = "No"
            course_number: float | None = 1.0
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            days_to_treatment_end: int | None = 1189
            days_to_treatment_start: int | None = 1140
            drug_category: str | None = "Platinum"
            embolic_agent: str | None = None
            initial_disease_status: str | None = "Recurrent Disease"
            lesions_treated_number: float | None = None
            margin_distance: float | None = 4.0
            margin_status: str | None = "Uninvolved"
            margins_involved_site: str | None = "Unknown"
            number_of_cycles: int | None = 3
            number_of_fractions: float | None = 25.0
            prescribed_dose: float | None = 75.0
            prescribed_dose_units: str | None = "mg/m2"
            pretreatment: str | None = "Thyroxine Withdrawal"
            protocol_identifier: str | None = "NWTS-5"
            radiosensitizing_agent: str | None = "No"
            reason_treatment_ended: str | None = "Course of Therapy Completed"
            reason_treatment_not_given: str | None = "Not Reported"
            regimen_or_line_of_therapy: str | None = "TIP"
            residual_disease: str | None = "R0"
            route_of_administration: tuple[str, ...] | None = ("Intravenous",)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            therapeutic_agents: str | None = "Gemcitabine Hydrochloride"
            therapeutic_level_achieved: str | None = None
            therapeutic_levels_achieved: str | None = "Unknown"
            therapeutic_target_level: str | None = ">14 mg/L"
            timepoint_category: str | None = "Last Contact"
            treatment_anatomic_sites: tuple[str, ...] | None = ("Not Reported",)
            treatment_dose: int | None = 6952
            treatment_dose_max: float | None = 1.0
            treatment_dose_units: str | None = "mg"
            treatment_duration: int | None = 3
            treatment_effect: str | None = None
            treatment_effect_indicator: str | None = None
            treatment_frequency: str | None = "Once Weekly"
            treatment_id: str | None = "treatment-0"
            treatment_intent_type: str | None = "Adjuvant"
            treatment_or_therapy: str | None = "no"
            treatment_outcome: str | None = "Not Reported"
            treatment_outcome_duration: int | None = 366
            treatment_type: str | None = "Radiation Therapy, NOS"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

        adrenal_hormone: str | None = None
        age_at_diagnosis: int | None = 31232
        ajcc_clinical_m: str | None = "MX"
        ajcc_clinical_n: str | None = "N0"
        ajcc_clinical_stage: str | None = "Stage IV"
        ajcc_clinical_t: str | None = "T1"
        ajcc_pathologic_m: str | None = "MX"
        ajcc_pathologic_n: str | None = "N0"
        ajcc_pathologic_stage: str | None = "Stage IB"
        ajcc_pathologic_t: str | None = "T1c"
        ajcc_serum_tumor_markers: str | None = "S2"
        ajcc_staging_system_edition: str | None = "7th"
        ann_arbor_b_symptoms: str | None = "Yes"
        ann_arbor_b_symptoms_described: str | None = None
        ann_arbor_clinical_stage: str | None = "Stage III"
        ann_arbor_extranodal_involvement: str | None = "No"
        ann_arbor_pathologic_stage: str | None = "Stage III"
        best_overall_response: str | None = "CR-Complete Response"
        burkitt_lymphoma_clinical_variant: str | None = "Endemic"
        calgb_risk_group: str | None = "Intermediate/Normal"
        cancer_detection_method: str | None = "Screening"
        child_pugh_classification: str | None = "B"
        clark_level: str | None = "IV"
        classification_of_tumor: str | None = "primary"
        cog_liver_stage: str | None = None
        cog_neuroblastoma_risk_group: str | None = "High Risk"
        cog_renal_stage: str | None = "Stage I"
        cog_rhabdomyosarcoma_risk_group: str | None = "High Risk"
        contiguous_organ_invaded: str | None = None
        days_to_best_overall_response: int | None = 850
        days_to_diagnosis: int | None = 505
        days_to_last_follow_up: float | None = 84.0
        days_to_last_known_disease_status: float | None = 510.0
        days_to_recurrence: float | None = 505.0
        diagnosis_id: str | None = "diagnosis-0"
        diagnosis_is_primary_disease: str | None = "True"
        double_expressor_lymphoma: str | None = "No"
        double_hit_lymphoma: str | None = "No"
        eln_risk_classification: str | None = "Adverse"
        enneking_msts_grade: str | None = "High Grade (G2)"
        enneking_msts_metastasis: str | None = "No Metastasis (M0)"
        enneking_msts_stage: str | None = None
        enneking_msts_tumor_site: str | None = "Extracompartmental (T2)"
        ensat_clinical_m: str | None = "M1"
        ensat_pathologic_n: str | None = "N1"
        ensat_pathologic_stage: str | None = "Stage IV"
        ensat_pathologic_t: str | None = "T2"
        esophageal_columnar_dysplasia_degree: str | None = "High Grade Dysplasia"
        esophageal_columnar_metaplasia_present: str | None = "Yes"
        fab_morphology_code: str | None = "M1"
        figo_stage: str | None = "Stage IIIC"
        figo_staging_edition_year: str | None = "2009"
        first_symptom_longest_duration: str | None = ">=181 Days"
        first_symptom_prior_to_diagnosis: str | None = "Sensory Changes"
        gastric_esophageal_junction_involvement: str | None = "Yes"
        gleason_grade_group: str | None = None
        gleason_grade_tertiary: str | None = "Pattern 5"
        gleason_patterns_percent: int | None = None
        gleason_score: int | None = 7
        goblet_cells_columnar_mucosa_present: str | None = "Yes"
        icd_10_code: str | None = "C56.9"
        igcccg_stage: str | None = "Good Prognosis"
        inpc_grade: str | None = "Undifferentiated or Poorly Differentiated"
        inpc_histologic_group: str | None = "Unfavorable"
        inrg_stage: str | None = "M"
        inss_stage: str | None = "Stage 4"
        international_prognostic_index: str | None = "High Risk"
        irs_group: str | None = "Group IV"
        irs_stage: str | None = None
        ishak_fibrosis_score: str | None = "1,2 - Portal Fibrosis"
        iss_stage: str | None = "I"
        last_known_disease_status: str | None = "Distant met recurrence/progression"
        laterality: str | None = "Left"
        margin_distance: float | None = 4.0
        margins_involved_site: str | None = "Unknown"
        masaoka_stage: str | None = "Stage IIb"
        max_tumor_bulk_site: str | None = "Cervical lymph nodes"
        medulloblastoma_molecular_classification: str | None = "Not Determined"
        melanoma_known_primary: str | None = "Yes"
        metastasis_at_diagnosis: str | None = "No Metastasis"
        method_of_diagnosis: str | None = "Surgical Resection"
        mitosis_karyorrhexis_index: str | None = "Intermediate"
        morphology: str | None = "8441/3"
        ovarian_specimen_status: str | None = "Ovarian Capsule Intact"
        ovarian_surface_involvement: str | None = "Present"
        pathology_details: tuple[PathologyDetail, ...] | None = (PathologyDetail(),)
        pediatric_kidney_staging: str | None = (
            "Nephrectomy specimen with tumor confined to the kidney; no distant metastasis"
        )
        peritoneal_fluid_cytological_status: str | None = "Unknown"
        primary_diagnosis: str | None = "Serous cystadenocarcinoma, NOS"
        primary_gleason_grade: str | None = "Pattern 3"
        prior_malignancy: str | None = "not reported"
        prior_treatment: str | None = "No"
        progression_or_recurrence: str | None = "Yes"
        residual_disease: str | None = "R0"
        satellite_nodule_present: str | None = "Absent"
        secondary_gleason_grade: str | None = "Pattern 4"
        site_of_resection_or_biopsy: str | None = "Ovary"
        sites_of_involvement: tuple[str, ...] | None = ("Central Lung",)
        sites_of_involvement_count: int | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        supratentorial_localization: str | None = "Cerebral Cortex"
        synchronous_malignancy: str | None = "Not Reported"
        tissue_or_organ_of_origin: str | None = "Ovary"
        treatments: tuple[Treatment, ...] | None = (Treatment(),)
        tumor_burden: float | None = 1.5
        tumor_confined_to_organ_of_origin: str | None = "Unknown"
        tumor_depth: float | None = 26.0
        tumor_focality: str | None = "Unifocal"
        tumor_grade: str | None = "G3"
        tumor_grade_category: str | None = "Three Tier"
        tumor_of_origin: str | None = "TEST-UNIT-diagnosis"
        tumor_regression_grade: str | None = "1"
        uicc_clinical_m: str | None = None
        uicc_clinical_n: str | None = None
        uicc_clinical_stage: str | None = "Stage IIA"
        uicc_clinical_t: str | None = None
        uicc_pathologic_m: str | None = "M0"
        uicc_pathologic_n: str | None = "N0"
        uicc_pathologic_stage: str | None = "Stage IIA"
        uicc_pathologic_t: str | None = "T3"
        uicc_staging_system_edition: str | None = "7th"
        ulceration_indicator: str | None = "Not Reported"
        weiss_assessment_findings: tuple[str, ...] | None = (
            "Atypical Mitotic Figures",
            "Cytoplasm presence <= to 25%",
            "Diffuse Architecture",
            "Mitotic Rate > 5/50 HPF",
            "Necrosis",
            "Nuclear Grade III or IV",
            "Sinusoid Invasion",
            "Venous Invasion",
        )
        weiss_assessment_score: str | None = "8"
        who_cns_grade: str | None = "Grade IV"
        who_nte_grade: str | None = None
        wilms_tumor_histologic_subtype: str | None = "Favorable"
        year_of_diagnosis: int | None = 2009

    @dataclasses.dataclass(frozen=True)
    class Exposure:
        age_at_last_exposure: int | None = 32
        age_at_onset: int | None = 17
        alcohol_days_per_week: float | None = 7.0
        alcohol_drinks_per_day: float | None = 5.0
        alcohol_frequency: str | None = None
        alcohol_history: str | None = "Yes"
        alcohol_intensity: str | None = "Drinker"
        alcohol_type: str | None = "Liquor"
        asbestos_exposure_type: str | None = "Crocidolite"
        chemical_exposure_type: tuple[str, ...] | None = ("Chemical Exposure, NOS",)
        cigarettes_per_day: float | None = 20.0
        environmental_tobacco_smoke_exposure: str | None = None
        exposure_duration: str | None = None
        exposure_duration_hrs_per_day: float | None = None
        exposure_duration_years: int | None = 23
        exposure_id: str | None = "exposure-0"
        exposure_source: str | None = "Occupational"
        exposure_type: str | None = "Tobacco"
        occupation_duration_years: int | None = 23
        occupation_type: tuple[str, ...] | None = ("Armed Forces Occupations, Other Ranks",)
        pack_years_smoked: float | None = 83.0
        parent_with_radiation_exposure: str | None = "Yes"
        secondhand_smoke_as_child: str | None = "Yes"
        smoking_frequency: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        time_between_waking_and_first_smoke: str | None = None
        tobacco_smoking_onset_year: int | None = 1946
        tobacco_smoking_quit_year: int | None = 1981
        tobacco_smoking_status: str | None = "Current Smoker"
        type_of_smoke_exposure: str | None = "Smoke exposure, NOS"
        type_of_tobacco_used: str | None = "Smokeless Tobacco"
        use_per_day: float | None = 2.0

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        family_history_id: str | None = "family-history-0"
        relationship_age_at_diagnosis: float | None = None
        relationship_gender: str | None = "female"
        relationship_primary_diagnosis: str | None = "Lung Cancer"
        relationship_sex_at_birth: str | None = "female"
        relationship_type: str | None = "Sibling"
        relative_deceased: str | None = None
        relative_smoker: str | None = None
        relative_with_cancer_history: str | None = "no"
        relatives_with_cancer_history_count: int | None = 1
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"

    @dataclasses.dataclass(frozen=True)
    class FollowUp:
        @dataclasses.dataclass(frozen=True)
        class MolecularTest:
            aa_change: str | None = "L858R"
            aneuploidy: str | None = "Monosomy"
            antigen: str | None = "HLA-DR"
            biospecimen_type: str | None = "Serum"
            biospecimen_volume: float | None = None
            blood_test_normal_range_lower: float | None = 0.5
            blood_test_normal_range_upper: float | None = 1.5
            cell_count: int | None = 113
            chromosomal_translocation: str | None = "t(6;11)(q27;q23)"
            chromosome: str | None = "chr17"
            chromosome_arm: str | None = "q"
            clonality: str | None = "Clonal"
            copy_number: float | None = 3.3
            cytoband: str | None = "1p36"
            days_to_test: int | None = -6
            exon: str | None = "17"
            gene_symbol: str | None = "KRAS"
            histone_family: str | None = "H3"
            histone_variant: str | None = None
            hpv_strain: str | None = "HPV70"
            intron: str | None = None
            laboratory_test: str | None = "Alpha Fetoprotein"
            loci_abnormal_count: int | None = 5
            loci_count: int | None = 5
            locus: str | None = None
            mismatch_repair_mutation: str | None = "Yes"
            mitotic_count: float | None = 32.0
            mitotic_total_area: float | None = None
            molecular_analysis_method: str | None = "Not Reported"
            molecular_consequence: str | None = "Exon Variant"
            molecular_test_id: str | None = "molecular-test-0"
            mutation_codon: str | None = "12"
            pathogenicity: str | None = "Pathogenic"
            ploidy: str | None = "Hyperdiploid"
            second_exon: str | None = "20"
            second_gene_symbol: str | None = "Not Reported"
            specialized_molecular_test: str | None = "Signal ratio"
            staining_intensity_scale: str | None = "4 Point Scale"
            staining_intensity_value: str | None = "2+"
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            test_analyte_type: str | None = None
            test_result: str | None = "Negative"
            test_units: str | None = "nmol/L"
            test_value: float | None = 4.0
            test_value_range: str | None = "30-39%"
            timepoint_category: str | None = "Last Contact"
            transcript: str | None = None
            variant_origin: str | None = "Germline"
            variant_type: str | None = "Mutation, NOS"
            zygosity: str | None = None

        @dataclasses.dataclass(frozen=True)
        class OtherClinicalAttribute:
            aids_risk_factors: str | None = "Candidiasis"
            bmi: float | None = 31.0
            body_surface_area: float | None = 81.7363192
            cd4_count: float | None = 621.0
            cdc_hiv_risk_factors: str | None = "Heterosexual Contact"
            comorbidities: tuple[str, ...] | None = ("Myasthenia Gravis",)
            comorbidity_method_of_diagnosis: str | None = "Radiology"
            days_to_comorbidity: int | None = -198
            days_to_risk_factor: int | None = -198
            diabetes_treatment_type: str | None = "Injected Insulin"
            dlco_ref_predictive_percent: float | None = 43.0
            exercise_frequency_weekly: str | None = None
            eye_color: str | None = "Unknown"
            fertility_history: str | None = None
            fev1_fvc_post_bronch_percent: float | None = 71.0
            fev1_fvc_pre_bronch_percent: float | None = 36.0
            fev1_ref_post_bronch_percent: float | None = 71.0
            fev1_ref_pre_bronch_percent: float | None = 31.0
            haart_treatment_indicator: str | None = "No"
            height: float | None = 148.0
            hepatitis_sustained_virological_response: str | None = "No"
            hiv_viral_load: float | None = 46414.0
            hormonal_contraceptive_type: str | None = "Unknown"
            hormonal_contraceptive_use: str | None = "Never Used"
            hormonal_replacement_therapy_status: str | None = "Yes"
            hormone_replacement_therapy_type: str | None = "Progesterone and Estrogen"
            hysterectomy_margins_involved: str | None = "None"
            hysterectomy_type: str | None = "Radical Hysterectomy"
            immunosuppressive_treatment_type: str | None = "Methotrexate"
            menopause_status: str | None = "Postmenopausal"
            myasthenia_gravis_classification: str | None = "Class V"
            nadir_cd4_count: float | None = 225.0
            nononcologic_therapeutic_agents: str | None = None
            number_of_pregnancies: str | None = "3"
            other_clinical_attribute_id: str | None = "other-clinical-attribute-0"
            oxygen_use_indicator: str | None = None
            oxygen_use_type: str | None = None
            pancreatitis_onset_year: int | None = 2018
            pregnancy_outcome: str | None = "Full Term Birth, NOS"
            pregnant_at_diagnosis: str | None = "No"
            premature_at_birth: str | None = None
            reflux_treatment_type: str | None = "Medically Treated"
            risk_factor_method_of_diagnosis: str | None = (
                "Both Clinical and Biochemical Assessments"
            )
            risk_factor_treatment: str | None = "Yes"
            risk_factors: tuple[str, ...] | None = ("Undescended Testis",)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            timepoint_category: str | None = "Last Contact"
            treatment_frequency: str | None = "Once Weekly"
            undescended_testis_corrected: str | None = "Yes"
            undescended_testis_corrected_age: int | None = None
            undescended_testis_corrected_age_range: str | None = "2-11 months"
            undescended_testis_corrected_laterality: str | None = "Right"
            undescended_testis_corrected_method: str | None = "Orchiopexy"
            undescended_testis_history: str | None = "Yes"
            undescended_testis_history_laterality: str | None = "Right"
            viral_hepatitis_serology_tests: tuple[str, ...] | None = ("Hepatitis C Antibody",)
            weeks_gestation_at_birth: float | None = None
            weight: float | None = 105.0

        adverse_event: str | None = "Platelet Count Decreased"
        adverse_event_grade: str | None = "Grade 3"
        barretts_esophagus_goblet_cells_present: str | None = "Unknown"
        cause_of_response: str | None = None
        days_to_adverse_event: int | None = None
        days_to_first_event: int | None = 383
        days_to_follow_up: int | None = 84
        days_to_imaging: int | None = 30
        days_to_progression: int | None = 153
        days_to_progression_free: int | None = 4717
        days_to_recurrence: int | None = 505
        discontiguous_lesion_count: int | None = 3
        disease_response: str | None = "WT-With Tumor"
        ecog_performance_status: str | None = "0"
        evidence_of_progression_type: str | None = "Biopsy with Histologic Confirmation"
        evidence_of_recurrence_type: str | None = "Convincing Image Source"
        first_event: str | None = "Relapse"
        follow_up_id: str | None = "follow-up-0"
        histologic_progression: str | None = "No"
        history_of_tumor: str | None = "No"
        history_of_tumor_type: str | None = "Phenochromocytoma or Paraganglioma"
        hormone_replacement_therapy_type: str | None = "Progesterone and Estrogen"
        imaging_anatomic_site: tuple[str, ...] | None = ("Pleura",)
        imaging_findings: str | None = "No Evidence of Extraprostatic Extension"
        imaging_result: str | None = "Negative"
        imaging_suv: float | None = 18.5
        imaging_suv_max: float | None = 6.0
        imaging_type: str | None = "CT Scan"
        karnofsky_performance_status: str | None = "100"
        molecular_tests: tuple[MolecularTest, ...] | None = (MolecularTest(),)
        other_clinical_attributes: tuple[OtherClinicalAttribute, ...] | None = (
            OtherClinicalAttribute(),
        )
        peritoneal_washing_results: str | None = "Positive"
        procedures_performed: str | None = "Colonoscopy"
        progression_or_recurrence: str | None = "Yes"
        progression_or_recurrence_anatomic_site: str | None = "Not Reported"
        progression_or_recurrence_type: str | None = "Unknown"
        recist_targeted_regions_number: int | None = None
        recist_targeted_regions_sum: float | None = None
        scan_tracer_used: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        timepoint_category: str | None = "Last Contact"
        treatment_emergent_adverse_event: str | None = None
        year_of_follow_up: int | None = 2013

    @dataclasses.dataclass(frozen=True)
    class Project:
        @dataclasses.dataclass(frozen=True)
        class Program:
            dbgap_accession_number: str | None = "phs000178"
            name: str | None = "Baylor College of Medicine"
            program_id: str | None = "program-0"

        dbgap_accession_number: str | None = "phs000178"
        disease_type: tuple[str, ...] | None = ("Cystic, Mucinous and Serous Neoplasms",)
        intended_release_date: str | None = None
        name: str | None = "Baylor College of Medicine"
        primary_site: tuple[str, ...] | None = ("Ovary",)
        program: Program | None = Program()
        project_id: str | None = "TEST-UNIT"
        releasable: str | None = "True"
        released: str | None = "True"
        state: str | None = "released"

    @dataclasses.dataclass(frozen=True)
    class Sample:
        biospecimen_anatomic_site: str | None = "Bone"
        biospecimen_laterality: str | None = "Unknown"
        catalog_reference: str | None = None
        current_weight: float | None = 0.386
        days_to_collection: int | None = 807
        days_to_sample_procurement: int | None = -18
        diagnosis_pathologically_confirmed: str | None = "Yes"
        distance_normal_to_tumor: str | None = None
        distributor_reference: str | None = None
        freezing_method: str | None = "None"
        growth_rate: int | None = None
        initial_weight: float | None = 130.0
        intermediate_dimension: float | None = 0.7
        longest_dimension: float | None = 0.8
        method_of_sample_procurement: str | None = "Needle Biopsy"
        passage_count: int | None = 7
        pathology_report_uuid: str | None = "00000000-0000-0000-0000-000000000000"
        preservation_method: str | None = "Unknown"
        sample_id: str | None = "sample-0"
        sample_ordinal: int | None = 1
        sample_type: str | None = "Blood Derived Normal"
        shortest_dimension: float | None = 0.6
        specimen_type: str | None = "Peripheral Blood NOS"
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        time_between_clamping_and_freezing: float | None = 16.0
        time_between_excision_and_freezing: float | None = 20.0
        tissue_collection_type: str | None = "Retrospective"
        tissue_type: str | None = "Normal"
        tumor_code_id: str | None = "50"
        tumor_descriptor: str | None = "Not Applicable"

    @dataclasses.dataclass(frozen=True)
    class TissueSourceSite:
        bcr_id: str | None = "IGC"
        code: str | None = "10"
        name: str | None = "Baylor College of Medicine"
        project: str | None = "Ovarian serous cystadenocarcinoma"
        tissue_source_site_id: str | None = "tissue-source-site-0"

    case_id: str | None = "case-0"
    consent_type: str | None = "Informed Consent"
    days_to_consent: int | None = 15
    days_to_lost_to_followup: int | None = 600
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    disease_type: str | None = "Cystic, Mucinous and Serous Neoplasms"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    follow_ups: tuple[FollowUp, ...] | None = (FollowUp(),)
    index_date: str | None = "Diagnosis"
    lost_to_followup: str | None = "No"
    primary_site: str | None = "Ovary"
    project: Project | None = Project()
    samples: tuple[Sample, ...] | None = (Sample(),)
    state: str | None = "released"
    submitter_id: str | None = "TEST-UNIT-submitter-0"
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    available_variation_data: tuple[str, ...] | None = None
