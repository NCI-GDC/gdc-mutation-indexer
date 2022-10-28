import dataclasses
from distutils import util
from typing import Iterable, Optional, Tuple

import more_itertools
from pyspark import sql

from tests.unit import utils


@dataclasses.dataclass(frozen=True)
class PathologyDetail:
    anaplasia_present: Optional[str] = None
    anaplasia_present_type: Optional[str] = None
    bone_marrow_malignant_cells: Optional[str] = None
    breslow_thickness: Optional[float] = None
    circumferential_resection_margin: Optional[float] = None
    columnar_mucosa_present: Optional[str] = None
    dysplasia_degree: Optional[str] = None
    dysplasia_type: Optional[str] = None
    greatest_tumor_dimension: Optional[float] = None
    gross_tumor_weight: Optional[float] = None
    largest_extrapelvic_peritoneal_focus: Optional[str] = None
    lymph_node_involved_site: Optional[str] = None
    lymph_node_involvement: Optional[str] = None
    lymph_nodes_positive: Optional[int] = None
    lymph_nodes_tested: Optional[int] = None
    lymphatic_invasion_present: Optional[str] = None
    margin_status: Optional[str] = None
    metaplasia_present: Optional[str] = None
    morphologic_architectural_pattern: Optional[str] = None
    necrosis_present: Optional[str] = None
    non_nodal_regional_disease: Optional[str] = None
    non_nodal_tumor_deposits: Optional[str] = None
    number_proliferating_cells: Optional[int] = None
    pathology_detail_id: Optional[str] = None
    percent_tumor_invasion: Optional[float] = None
    perineural_invasion_present: Optional[str] = None
    peripancreatic_lymph_nodes_positive: Optional[str] = None
    peripancreatic_lymph_nodes_tested: Optional[int] = None
    prostatic_chips_positive_count: Optional[float] = None
    prostatic_chips_total_count: Optional[float] = None
    prostatic_involvement_percent: Optional[float] = None
    state: Optional[str] = None
    submitter_id: Optional[str] = None
    transglottic_extension: Optional[str] = None
    tumor_largest_dimension_diameter: Optional[float] = None
    vascular_invasion_present: Optional[str] = None
    vascular_invasion_type: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class DataCategory:
    data_category: Optional[str] = "Structural Variation"
    file_count: Optional[int] = 4


@dataclasses.dataclass(frozen=True)
class ExperimentalStrategy:
    experimental_strategy: Optional[str] = "RNA-Seq"
    file_count: Optional[int] = 14


@dataclasses.dataclass(frozen=True)
class Summary:
    data_categories: Tuple[DataCategory, ...] = (DataCategory(),)
    experimental_strategies: Tuple[ExperimentalStrategy, ...] = (
        ExperimentalStrategy(),
    )
    file_count: Optional[int] = 68
    file_size: Optional[int] = 447068180309


@dataclasses.dataclass(frozen=True)
class FamilyHistory:
    family_history_id: Optional[str] = "6d2bf40e-b840-4cd9-9f64-0a3177020527"
    relationship_age_at_diagnosis: Optional[float] = None
    relationship_gender: Optional[str] = None
    relationship_primary_diagnosis: Optional[str] = "Rectal Cancer"
    relationship_type: Optional[str] = None
    relative_with_cancer_history: Optional[str] = "yes"
    relatives_with_cancer_history_count: Optional[int] = None
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_family_history"


@dataclasses.dataclass(frozen=True)
class TissueSourceSite:
    bcr_id: Optional[str] = None
    code: Optional[str] = None
    name: Optional[str] = None
    project: Optional[str] = None
    tissue_source_site_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Program:
    dbgap_accession_number: Optional[str] = None
    name: Optional[str] = "HCMI"
    program_id: Optional[str] = "a5448c11-d46a-56aa-a5e1-5c1aa06404df"


@dataclasses.dataclass(frozen=True)
class Project:
    dbgap_accession_number: Optional[str] = "phs001486"
    disease_type: Tuple[str, ...] = (
        "Epithelial Neoplasms, NOS",
        "Complex Mixed and Stromal Neoplasms",
        "Gliomas",
        "Miscellaneous Bone Tumors",
        "Ductal and Lobular Neoplasms",
        "Complex Epithelial Neoplasms",
        "Myomatous Neoplasms",
        "Nevi and Melanomas",
        "Soft Tissue Tumors and Sarcomas, NOS",
        "Cystic, Mucinous and Serous Neoplasms",
        "Adenomas and Adenocarcinomas",
    )
    intended_release_date: Optional[str] = None
    name: Optional[
        str
    ] = "NCI Cancer Model Development for the Human Cancer Model Initiative"
    primary_site: Tuple[str, ...] = (
        "Connective, subcutaneous and other soft tissues",
        "Breast",
        "Kidney",
        "Bronchus and lung",
        "Small intestine",
        "Brain",
        "Skin",
        "Esophagus",
        "Pancreas",
        "Colon",
        "Bones, joints and articular cartilage of other and unspecified sites",
        "Liver and intrahepatic bile ducts",
        "Rectum",
        "Other and unspecified parts of biliary tract",
        "Stomach",
        "Rectosigmoid junction",
    )
    program: Program = Program()
    project_id: Optional[str] = "HCMI-CMDC"


@dataclasses.dataclass(frozen=True)
class Treatment:
    chemo_concurrent_to_radiation: Optional[str] = None
    days_to_treatment_end: Optional[int] = None
    days_to_treatment_start: Optional[int] = 53
    initial_disease_status: Optional[str] = "Progressive Disease"
    number_of_cycles: Optional[int] = None
    reason_treatment_ended: Optional[str] = None
    regimen_or_line_of_therapy: Optional[str] = None
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_treatment4"
    therapeutic_agents: Optional[str] = "Oxaliplatin"
    treatment_anatomic_site: Optional[str] = None
    treatment_dose: Optional[int] = None
    treatment_frequency: Optional[str] = None
    treatment_id: Optional[str] = "0619e104-8110-441b-97ef-a6a13d6823bb"
    treatment_intent_type: Optional[str] = "Maintenance Therapy"
    treatment_or_therapy: Optional[str] = "yes"
    treatment_outcome: Optional[str] = "Progressive Disease"
    treatment_type: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Diagnosis:
    age_at_diagnosis: Optional[int] = 17316
    ajcc_clinical_m: Optional[str] = None
    ajcc_clinical_n: Optional[str] = None
    ajcc_clinical_stage: Optional[str] = None
    ajcc_clinical_t: Optional[str] = None
    ajcc_pathologic_m: Optional[str] = "M1a"
    ajcc_pathologic_n: Optional[str] = "N1"
    ajcc_pathologic_stage: Optional[str] = None
    ajcc_pathologic_t: Optional[str] = "T4"
    ajcc_staging_system_edition: Optional[str] = "7th"
    ann_arbor_b_symptoms: Optional[str] = None
    ann_arbor_clinical_stage: Optional[str] = None
    ann_arbor_extranodal_involvement: Optional[str] = None
    ann_arbor_pathologic_stage: Optional[str] = None
    best_overall_response: Optional[str] = None
    burkitt_lymphoma_clinical_variant: Optional[str] = None
    classification_of_tumor: Optional[str] = "metastasis"
    cog_renal_stage: Optional[str] = None
    cog_rhabdomyosarcoma_risk_group: Optional[str] = None
    days_to_best_overall_response: Optional[int] = None
    days_to_diagnosis: Optional[int] = 0
    days_to_last_follow_up: Optional[float] = None
    days_to_last_known_disease_status: Optional[float] = None
    days_to_recurrence: Optional[float] = None
    diagnosis_id: Optional[str] = "a7f019db-c623-4d56-94e9-102cc4574f88"
    eln_risk_classification: Optional[str] = None
    esophageal_columnar_dysplasia_degree: Optional[str] = None
    esophageal_columnar_metaplasia_present: Optional[str] = None
    figo_stage: Optional[str] = None
    figo_staging_edition_year: Optional[str] = None
    gastric_esophageal_junction_involvement: Optional[str] = None
    goblet_cells_columnar_mucosa_present: Optional[str] = None
    icd_10_code: Optional[str] = "C79.3"
    igcccg_stage: Optional[str] = None
    inss_stage: Optional[str] = None
    international_prognostic_index: Optional[str] = None
    irs_group: Optional[str] = None
    iss_stage: Optional[str] = None
    last_known_disease_status: Optional[str] = None
    laterality: Optional[str] = None
    masaoka_stage: Optional[str] = None
    metastasis_at_diagnosis: Optional[str] = "Unknown"
    metastasis_at_diagnosis_site: Optional[str] = None
    method_of_diagnosis: Optional[str] = None
    micropapillary_features: Optional[str] = None
    morphology: Optional[str] = "8140/3"
    pathology_details: Tuple[PathologyDetail, ...] = (PathologyDetail(),)
    pregnant_at_diagnosis: Optional[str] = None
    primary_diagnosis: Optional[str] = "Adenocarcinoma, NOS"
    primary_gleason_grade: Optional[str] = None
    prior_malignancy: Optional[str] = "no"
    prior_treatment: Optional[str] = "Yes"
    progression_or_recurrence: Optional[str] = None
    residual_disease: Optional[str] = None
    satellite_nodule_present: Optional[str] = None
    secondary_gleason_grade: Optional[str] = None
    site_of_resection_or_biopsy: Optional[str] = "Brain, NOS"
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_diagnosis"
    synchronous_malignancy: Optional[str] = None
    tissue_or_organ_of_origin: Optional[str] = "Rectum, NOS"
    treatments: Tuple[Treatment, ...] = (Treatment(),)
    tumor_confined_to_organ_of_origin: Optional[str] = None
    tumor_focality: Optional[str] = None
    tumor_grade: Optional[str] = "GX"
    wilms_tumor_histologic_subtype: Optional[str] = None
    year_of_diagnosis: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class Demographic:
    age_at_index: Optional[int] = None
    age_is_obfuscated: Optional[str] = False
    cause_of_death: Optional[str] = "Cancer Related"
    days_to_birth: Optional[int] = -17316
    days_to_death: Optional[int] = 1011
    demographic_id: Optional[str] = "c1e37539-cdf7-4707-8913-fafb8146e838"
    ethnicity: Optional[str] = "Unknown"
    gender: Optional[str] = "female"
    race: Optional[str] = "white"
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_demographic"
    vital_status: Optional[str] = "Dead"
    year_of_birth: Optional[int] = 1963
    year_of_death: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class Exposure:
    alcohol_days_per_week: Optional[float] = None
    alcohol_history: Optional[str] = None
    alcohol_intensity: Optional[str] = None
    asbestos_exposure: Optional[str] = None
    cigarettes_per_day: Optional[float] = None
    exposure_id: Optional[str] = "a0ce5ab0-e9df-459a-960f-85fd4203ec62"
    exposure_type: Optional[str] = None
    pack_years_smoked: Optional[float] = None
    parent_with_radiation_exposure: Optional[str] = None
    radon_exposure: Optional[str] = None
    secondhand_smoke_as_child: Optional[str] = None
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_exposure"
    tobacco_smoking_onset_year: Optional[int] = None
    tobacco_smoking_quit_year: Optional[int] = None
    tobacco_smoking_status: Optional[str] = "4"
    type_of_smoke_exposure: Optional[str] = None
    type_of_tobacco_used: Optional[str] = None
    years_smoked: Optional[float] = None


@dataclasses.dataclass(frozen=True)
class MolecularTest:
    aa_change: Optional[str] = "V600E"
    antigen: Optional[str] = None
    biospecimen_type: Optional[str] = None
    biospecimen_volume: Optional[float] = None
    blood_test_normal_range_lower: Optional[float] = None
    blood_test_normal_range_upper: Optional[float] = None
    cell_count: Optional[int] = None
    chromosome: Optional[str] = None
    clonality: Optional[str] = None
    copy_number: Optional[float] = None
    cytoband: Optional[str] = None
    days_to_test: Optional[int] = None
    exon: Optional[str] = None
    gene_symbol: Optional[str] = "BRAF"
    histone_family: Optional[str] = None
    histone_variant: Optional[str] = None
    intron: Optional[str] = None
    laboratory_test: Optional[str] = None
    loci_abnormal_count: Optional[int] = None
    loci_count: Optional[int] = None
    locus: Optional[str] = None
    mismatch_repair_mutation: Optional[str] = None
    mitotic_count: Optional[float] = None
    mitotic_total_area: Optional[float] = None
    molecular_analysis_method: Optional[str] = "Not Reported"
    molecular_consequence: Optional[str] = None
    molecular_test_id: Optional[str] = "f21c04ea-06f9-45cb-bed6-489f7ce2880e"
    pathogenicity: Optional[str] = None
    ploidy: Optional[str] = None
    second_exon: Optional[str] = None
    second_gene_symbol: Optional[str] = None
    specialized_molecular_test: Optional[str] = None
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_molecular_test"
    test_analyte_type: Optional[str] = None
    test_result: Optional[str] = "Positive"
    test_units: Optional[str] = None
    test_value: Optional[float] = None
    transcript: Optional[str] = None
    variant_origin: Optional[str] = None
    variant_type: Optional[str] = None
    zygosity: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class FollowUp:
    adverse_event: Optional[str] = None
    adverse_event_grade: Optional[str] = None
    aids_risk_factors: Optional[str] = None
    barretts_esophagus_goblet_cells_present: Optional[str] = None
    bmi: Optional[float] = None
    body_surface_area: Optional[float] = None
    cause_of_response: Optional[str] = None
    cd4_count: Optional[float] = None
    cdc_hiv_risk_factors: Optional[str] = None
    comorbidity: Optional[str] = None
    comorbidity_method_of_diagnosis: Optional[str] = None
    days_to_adverse_event: Optional[int] = None
    days_to_comorbidity: Optional[int] = None
    days_to_follow_up: Optional[int] = 0
    days_to_imaging: Optional[int] = None
    days_to_progression: Optional[int] = None
    days_to_progression_free: Optional[int] = None
    days_to_recurrence: Optional[int] = None
    diabetes_treatment_type: Optional[str] = None
    disease_response: Optional[str] = None
    dlco_ref_predictive_percent: Optional[float] = None
    ecog_performance_status: Optional[str] = None
    evidence_of_recurrence_type: Optional[str] = None
    eye_color: Optional[str] = None
    fev1_fvc_post_bronch_percent: Optional[float] = None
    fev1_fvc_pre_bronch_percent: Optional[float] = None
    fev1_ref_post_bronch_percent: Optional[float] = None
    fev1_ref_pre_bronch_percent: Optional[float] = None
    follow_up_id: Optional[str] = "c2523f92-b185-4fc0-adb1-ab3adf8bf3cc"
    haart_treatment_indicator: Optional[str] = None
    height: Optional[float] = 165.0
    hepatitis_sustained_virological_response: Optional[str] = None
    history_of_tumor: Optional[str] = None
    history_of_tumor_type: Optional[str] = None
    hiv_viral_load: Optional[float] = None
    hormonal_contraceptive_type: Optional[str] = None
    hormonal_contraceptive_use: Optional[str] = None
    hormone_replacement_therapy_type: Optional[str] = None
    hpv_positive_type: Optional[str] = None
    hysterectomy_margins_involved: Optional[str] = None
    hysterectomy_type: Optional[str] = None
    imaging_result: Optional[str] = None
    imaging_type: Optional[str] = None
    immunosuppressive_treatment_type: Optional[str] = None
    karnofsky_performance_status: Optional[str] = None
    menopause_status: Optional[str] = None
    molecular_tests: Tuple[MolecularTest, ...] = (MolecularTest(),)
    nadir_cd4_count: Optional[float] = None
    pancreatitis_onset_year: Optional[int] = None
    pregnancy_outcome: Optional[str] = None
    procedures_performed: Optional[str] = None
    progression_or_recurrence: Optional[str] = None
    progression_or_recurrence_anatomic_site: Optional[str] = None
    progression_or_recurrence_type: Optional[str] = None
    recist_targeted_regions_number: Optional[int] = None
    recist_targeted_regions_sum: Optional[float] = None
    reflux_treatment_type: Optional[str] = None
    risk_factor: Optional[str] = None
    risk_factor_treatment: Optional[str] = None
    scan_tracer_used: Optional[str] = None
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18_follow_up"
    undescended_testis_corrected: Optional[str] = None
    undescended_testis_corrected_age: Optional[int] = None
    undescended_testis_corrected_laterality: Optional[str] = None
    undescended_testis_corrected_method: Optional[str] = None
    undescended_testis_history: Optional[str] = None
    undescended_testis_history_laterality: Optional[str] = None
    viral_hepatitis_serologies: Optional[str] = None
    weight: Optional[float] = 79.8


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: Optional[str] = "case-0"
    consent_type: Optional[str] = None
    days_to_consent: Optional[int] = None
    days_to_lost_to_followup: Optional[int] = None
    demographic: Demographic = Demographic()
    diagnoses: Tuple[Diagnosis, ...] = (Diagnosis(),)
    disease_type: Optional[str] = "Adenomas and Adenocarcinomas"
    exposures: Tuple[Exposure, ...] = (Exposure(),)
    family_histories: Tuple[FamilyHistory, ...] = (FamilyHistory(),)
    follow_ups: Tuple[FollowUp, ...] = (FollowUp(),)
    index_date: Optional[str] = "Diagnosis"
    lost_to_followup: Optional[str] = None
    primary_site: Optional[str] = "Rectum"
    project: Project = Project()
    state: Optional[str] = "released"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18"
    summary: Summary = Summary()
    tissue_source_site: TissueSourceSite = TissueSourceSite()


def assert_demographic_translated(
    result_demographic: sql.Row, demographic: Demographic
):
    assert result_demographic.age_at_index == demographic.age_at_index
    assert (
        util.strtobool(result_demographic.age_is_obfuscated)
        == demographic.age_is_obfuscated
    )
    assert result_demographic.cause_of_death == demographic.cause_of_death
    assert result_demographic.days_to_birth == demographic.days_to_birth
    assert result_demographic.days_to_death == demographic.days_to_death
    assert result_demographic.demographic_id == demographic.demographic_id
    assert result_demographic.ethnicity == demographic.ethnicity
    assert result_demographic.gender == demographic.gender
    assert result_demographic.race == demographic.race
    assert result_demographic.state == demographic.state
    assert result_demographic.submitter_id == demographic.submitter_id
    assert result_demographic.vital_status == demographic.vital_status
    assert result_demographic.year_of_birth == demographic.year_of_birth
    assert result_demographic.year_of_death == demographic.year_of_death


def assert_pathology_details_translated(
    result_pathology_details: Iterable[sql.Row],
    pathology_details: Iterable[PathologyDetail],
) -> None:
    result_pathology_detail = more_itertools.one(result_pathology_details)
    pathology_detail = more_itertools.one(pathology_details)

    assert (
        result_pathology_detail.anaplasia_present == pathology_detail.anaplasia_present
    )
    assert (
        result_pathology_detail.anaplasia_present_type
        == pathology_detail.anaplasia_present_type
    )
    assert (
        result_pathology_detail.bone_marrow_malignant_cells
        == pathology_detail.bone_marrow_malignant_cells
    )
    utils.assert_float_equal(
        result_pathology_detail.breslow_thickness, pathology_detail.breslow_thickness
    )
    utils.assert_float_equal(
        result_pathology_detail.circumferential_resection_margin,
        pathology_detail.circumferential_resection_margin,
    )
    assert (
        result_pathology_detail.columnar_mucosa_present
        == pathology_detail.columnar_mucosa_present
    )
    assert result_pathology_detail.dysplasia_degree == pathology_detail.dysplasia_degree
    assert result_pathology_detail.dysplasia_type == pathology_detail.dysplasia_type
    utils.assert_float_equal(
        result_pathology_detail.greatest_tumor_dimension,
        pathology_detail.greatest_tumor_dimension,
    )
    utils.assert_float_equal(
        result_pathology_detail.gross_tumor_weight, pathology_detail.gross_tumor_weight
    )
    assert (
        result_pathology_detail.largest_extrapelvic_peritoneal_focus
        == pathology_detail.largest_extrapelvic_peritoneal_focus
    )
    assert (
        result_pathology_detail.lymph_node_involved_site
        == pathology_detail.lymph_node_involved_site
    )
    assert (
        result_pathology_detail.lymph_node_involvement
        == pathology_detail.lymph_node_involvement
    )
    assert (
        result_pathology_detail.lymph_nodes_positive
        == pathology_detail.lymph_nodes_positive
    )
    assert (
        result_pathology_detail.lymph_nodes_tested
        == pathology_detail.lymph_nodes_tested
    )
    assert (
        result_pathology_detail.lymphatic_invasion_present
        == pathology_detail.lymphatic_invasion_present
    )
    assert result_pathology_detail.margin_status == pathology_detail.margin_status
    assert (
        result_pathology_detail.metaplasia_present
        == pathology_detail.metaplasia_present
    )
    assert (
        result_pathology_detail.morphologic_architectural_pattern
        == pathology_detail.morphologic_architectural_pattern
    )
    assert result_pathology_detail.necrosis_present == pathology_detail.necrosis_present
    assert (
        result_pathology_detail.non_nodal_regional_disease
        == pathology_detail.non_nodal_regional_disease
    )
    assert (
        result_pathology_detail.non_nodal_tumor_deposits
        == pathology_detail.non_nodal_tumor_deposits
    )
    assert (
        result_pathology_detail.number_proliferating_cells
        == pathology_detail.number_proliferating_cells
    )
    assert (
        result_pathology_detail.pathology_detail_id
        == pathology_detail.pathology_detail_id
    )
    utils.assert_float_equal(
        result_pathology_detail.percent_tumor_invasion,
        pathology_detail.percent_tumor_invasion,
    )
    assert (
        result_pathology_detail.perineural_invasion_present
        == pathology_detail.perineural_invasion_present
    )
    assert (
        result_pathology_detail.peripancreatic_lymph_nodes_positive
        == pathology_detail.peripancreatic_lymph_nodes_positive
    )
    assert (
        result_pathology_detail.peripancreatic_lymph_nodes_tested
        == pathology_detail.peripancreatic_lymph_nodes_tested
    )
    utils.assert_float_equal(
        result_pathology_detail.prostatic_chips_positive_count,
        pathology_detail.prostatic_chips_positive_count,
    )
    utils.assert_float_equal(
        result_pathology_detail.prostatic_chips_total_count,
        pathology_detail.prostatic_chips_total_count,
    )
    utils.assert_float_equal(
        result_pathology_detail.prostatic_involvement_percent,
        pathology_detail.prostatic_involvement_percent,
    )
    assert result_pathology_detail.state == pathology_detail.state
    assert result_pathology_detail.submitter_id == pathology_detail.submitter_id
    assert (
        result_pathology_detail.transglottic_extension
        == pathology_detail.transglottic_extension
    )
    utils.assert_float_equal(
        result_pathology_detail.tumor_largest_dimension_diameter,
        pathology_detail.tumor_largest_dimension_diameter,
    )
    assert (
        result_pathology_detail.vascular_invasion_present
        == pathology_detail.vascular_invasion_present
    )
    assert (
        result_pathology_detail.vascular_invasion_type
        == pathology_detail.vascular_invasion_type
    )


def assert_treatments_translated(
    result_treatments: Iterable[sql.Row], treatments: Iterable[Treatment]
) -> None:
    result_treatment = more_itertools.one(result_treatments)
    treatment = more_itertools.one(treatments)

    assert (
        result_treatment.chemo_concurrent_to_radiation
        == treatment.chemo_concurrent_to_radiation
    )
    assert result_treatment.days_to_treatment_end == treatment.days_to_treatment_end
    assert result_treatment.days_to_treatment_start == treatment.days_to_treatment_start
    assert result_treatment.initial_disease_status == treatment.initial_disease_status
    assert result_treatment.number_of_cycles == treatment.number_of_cycles
    assert result_treatment.reason_treatment_ended == treatment.reason_treatment_ended
    assert (
        result_treatment.regimen_or_line_of_therapy
        == treatment.regimen_or_line_of_therapy
    )
    assert result_treatment.state == treatment.state
    assert result_treatment.submitter_id == treatment.submitter_id
    assert result_treatment.therapeutic_agents == treatment.therapeutic_agents
    assert result_treatment.treatment_anatomic_site == treatment.treatment_anatomic_site
    assert result_treatment.treatment_dose == treatment.treatment_dose
    assert result_treatment.treatment_frequency == treatment.treatment_frequency
    assert result_treatment.treatment_id == treatment.treatment_id
    assert result_treatment.treatment_intent_type == treatment.treatment_intent_type
    assert result_treatment.treatment_or_therapy == treatment.treatment_or_therapy
    assert result_treatment.treatment_outcome == treatment.treatment_outcome
    assert result_treatment.treatment_type == treatment.treatment_type


def assert_diagnoses_translated(
    result_diagnoses: Iterable[sql.Row], diagnoses: Iterable[Diagnosis]
) -> None:
    result_diagnosis = more_itertools.one(result_diagnoses)
    diagnosis = more_itertools.one(diagnoses)

    assert result_diagnosis.age_at_diagnosis == diagnosis.age_at_diagnosis
    assert result_diagnosis.ajcc_clinical_m == diagnosis.ajcc_clinical_m
    assert result_diagnosis.ajcc_clinical_n == diagnosis.ajcc_clinical_n
    assert result_diagnosis.ajcc_clinical_stage == diagnosis.ajcc_clinical_stage
    assert result_diagnosis.ajcc_clinical_t == diagnosis.ajcc_clinical_t
    assert result_diagnosis.ajcc_pathologic_m == diagnosis.ajcc_pathologic_m
    assert result_diagnosis.ajcc_pathologic_n == diagnosis.ajcc_pathologic_n
    assert result_diagnosis.ajcc_pathologic_stage == diagnosis.ajcc_pathologic_stage
    assert result_diagnosis.ajcc_pathologic_t == diagnosis.ajcc_pathologic_t
    assert (
        result_diagnosis.ajcc_staging_system_edition
        == diagnosis.ajcc_staging_system_edition
    )
    assert result_diagnosis.ann_arbor_b_symptoms == diagnosis.ann_arbor_b_symptoms
    assert (
        result_diagnosis.ann_arbor_clinical_stage == diagnosis.ann_arbor_clinical_stage
    )
    assert (
        result_diagnosis.ann_arbor_extranodal_involvement
        == diagnosis.ann_arbor_extranodal_involvement
    )
    assert (
        result_diagnosis.ann_arbor_pathologic_stage
        == diagnosis.ann_arbor_pathologic_stage
    )
    assert result_diagnosis.best_overall_response == diagnosis.best_overall_response
    assert (
        result_diagnosis.burkitt_lymphoma_clinical_variant
        == diagnosis.burkitt_lymphoma_clinical_variant
    )
    assert result_diagnosis.classification_of_tumor == diagnosis.classification_of_tumor
    assert result_diagnosis.cog_renal_stage == diagnosis.cog_renal_stage
    assert (
        result_diagnosis.cog_rhabdomyosarcoma_risk_group
        == diagnosis.cog_rhabdomyosarcoma_risk_group
    )
    assert (
        result_diagnosis.days_to_best_overall_response
        == diagnosis.days_to_best_overall_response
    )
    assert result_diagnosis.days_to_diagnosis == diagnosis.days_to_diagnosis
    utils.assert_float_equal(
        result_diagnosis.days_to_last_follow_up, diagnosis.days_to_last_follow_up
    )
    utils.assert_float_equal(
        result_diagnosis.days_to_last_known_disease_status,
        diagnosis.days_to_last_known_disease_status,
    )
    utils.assert_float_equal(
        result_diagnosis.days_to_recurrence, diagnosis.days_to_recurrence
    )
    assert result_diagnosis.diagnosis_id == diagnosis.diagnosis_id
    assert result_diagnosis.eln_risk_classification == diagnosis.eln_risk_classification
    assert (
        result_diagnosis.esophageal_columnar_dysplasia_degree
        == diagnosis.esophageal_columnar_dysplasia_degree
    )
    assert (
        result_diagnosis.esophageal_columnar_metaplasia_present
        == diagnosis.esophageal_columnar_metaplasia_present
    )
    assert result_diagnosis.figo_stage == diagnosis.figo_stage
    assert (
        result_diagnosis.figo_staging_edition_year
        == diagnosis.figo_staging_edition_year
    )
    assert (
        result_diagnosis.gastric_esophageal_junction_involvement
        == diagnosis.gastric_esophageal_junction_involvement
    )
    assert (
        result_diagnosis.goblet_cells_columnar_mucosa_present
        == diagnosis.goblet_cells_columnar_mucosa_present
    )
    assert result_diagnosis.icd_10_code == diagnosis.icd_10_code
    assert result_diagnosis.igcccg_stage == diagnosis.igcccg_stage
    assert result_diagnosis.inss_stage == diagnosis.inss_stage
    assert (
        result_diagnosis.international_prognostic_index
        == diagnosis.international_prognostic_index
    )
    assert result_diagnosis.irs_group == diagnosis.irs_group
    assert result_diagnosis.iss_stage == diagnosis.iss_stage
    assert (
        result_diagnosis.last_known_disease_status
        == diagnosis.last_known_disease_status
    )
    assert result_diagnosis.laterality == diagnosis.laterality
    assert result_diagnosis.masaoka_stage == diagnosis.masaoka_stage
    assert result_diagnosis.metastasis_at_diagnosis == diagnosis.metastasis_at_diagnosis
    assert (
        result_diagnosis.metastasis_at_diagnosis_site
        == diagnosis.metastasis_at_diagnosis_site
    )
    assert result_diagnosis.method_of_diagnosis == diagnosis.method_of_diagnosis
    assert result_diagnosis.micropapillary_features == diagnosis.micropapillary_features
    assert result_diagnosis.morphology == diagnosis.morphology
    assert result_diagnosis.pregnant_at_diagnosis == diagnosis.pregnant_at_diagnosis
    assert result_diagnosis.primary_diagnosis == diagnosis.primary_diagnosis
    assert result_diagnosis.primary_gleason_grade == diagnosis.primary_gleason_grade
    assert result_diagnosis.prior_malignancy == diagnosis.prior_malignancy
    assert result_diagnosis.prior_treatment == diagnosis.prior_treatment
    assert (
        result_diagnosis.progression_or_recurrence
        == diagnosis.progression_or_recurrence
    )
    assert result_diagnosis.residual_disease == diagnosis.residual_disease
    assert (
        result_diagnosis.satellite_nodule_present == diagnosis.satellite_nodule_present
    )
    assert result_diagnosis.secondary_gleason_grade == diagnosis.secondary_gleason_grade
    assert (
        result_diagnosis.site_of_resection_or_biopsy
        == diagnosis.site_of_resection_or_biopsy
    )
    assert result_diagnosis.state == diagnosis.state
    assert result_diagnosis.submitter_id == diagnosis.submitter_id
    assert result_diagnosis.synchronous_malignancy == diagnosis.synchronous_malignancy
    assert (
        result_diagnosis.tissue_or_organ_of_origin
        == diagnosis.tissue_or_organ_of_origin
    )
    assert (
        result_diagnosis.tumor_confined_to_organ_of_origin
        == diagnosis.tumor_confined_to_organ_of_origin
    )
    assert result_diagnosis.tumor_focality == diagnosis.tumor_focality
    assert result_diagnosis.tumor_grade == diagnosis.tumor_grade
    assert (
        result_diagnosis.wilms_tumor_histologic_subtype
        == diagnosis.wilms_tumor_histologic_subtype
    )
    assert result_diagnosis.year_of_diagnosis == diagnosis.year_of_diagnosis

    assert_pathology_details_translated(
        result_diagnosis.pathology_details, diagnosis.pathology_details
    )
    assert_treatments_translated(result_diagnosis.treatments, diagnosis.treatments)


def assert_exposures_translated(
    result_exposures: Iterable[sql.Row], exposures: Iterable[Exposure]
) -> None:
    result_exposure = more_itertools.one(result_exposures)
    exposure = more_itertools.one(exposures)

    utils.assert_float_equal(
        result_exposure.alcohol_days_per_week, exposure.alcohol_days_per_week
    )
    assert result_exposure.alcohol_history == exposure.alcohol_history
    assert result_exposure.alcohol_intensity == exposure.alcohol_intensity
    assert result_exposure.asbestos_exposure == exposure.asbestos_exposure
    utils.assert_float_equal(
        result_exposure.cigarettes_per_day, exposure.cigarettes_per_day
    )
    assert result_exposure.exposure_id == exposure.exposure_id
    assert result_exposure.exposure_type == exposure.exposure_type
    utils.assert_float_equal(
        result_exposure.pack_years_smoked, exposure.pack_years_smoked
    )
    assert (
        result_exposure.parent_with_radiation_exposure
        == exposure.parent_with_radiation_exposure
    )
    assert result_exposure.radon_exposure == exposure.radon_exposure
    assert (
        result_exposure.secondhand_smoke_as_child == exposure.secondhand_smoke_as_child
    )
    assert result_exposure.state == exposure.state
    assert result_exposure.submitter_id == exposure.submitter_id
    assert (
        result_exposure.tobacco_smoking_onset_year
        == exposure.tobacco_smoking_onset_year
    )
    assert (
        result_exposure.tobacco_smoking_quit_year == exposure.tobacco_smoking_quit_year
    )
    assert result_exposure.tobacco_smoking_status == exposure.tobacco_smoking_status
    assert result_exposure.type_of_smoke_exposure == exposure.type_of_smoke_exposure
    assert result_exposure.type_of_tobacco_used == exposure.type_of_tobacco_used
    utils.assert_float_equal(result_exposure.years_smoked, exposure.years_smoked)


def assert_family_histories_translated(
    result_family_histories: Iterable[sql.Row],
    family_histories: Iterable[FamilyHistory],
) -> None:
    result_family_history = more_itertools.one(result_family_histories)
    family_history = more_itertools.one(family_histories)

    assert result_family_history.family_history_id == family_history.family_history_id
    utils.assert_float_equal(
        result_family_history.relationship_age_at_diagnosis,
        family_history.relationship_age_at_diagnosis,
    )
    assert (
        result_family_history.relationship_gender == family_history.relationship_gender
    )
    assert (
        result_family_history.relationship_primary_diagnosis
        == family_history.relationship_primary_diagnosis
    )
    assert result_family_history.relationship_type == family_history.relationship_type
    assert (
        result_family_history.relative_with_cancer_history
        == family_history.relative_with_cancer_history
    )
    assert (
        result_family_history.relatives_with_cancer_history_count
        == family_history.relatives_with_cancer_history_count
    )
    assert result_family_history.submitter_id == family_history.submitter_id


def assert_molecular_tests_translated(
    result_molecular_tests: Iterable[sql.Row], molecular_tests: Iterable[MolecularTest]
) -> None:
    result_molecular_test = more_itertools.one(result_molecular_tests)
    molecular_test = more_itertools.one(molecular_tests)

    assert result_molecular_test.aa_change == molecular_test.aa_change
    assert result_molecular_test.antigen == molecular_test.antigen
    assert result_molecular_test.biospecimen_type == molecular_test.biospecimen_type
    utils.assert_float_equal(
        result_molecular_test.biospecimen_volume, molecular_test.biospecimen_volume
    )
    utils.assert_float_equal(
        result_molecular_test.blood_test_normal_range_lower,
        molecular_test.blood_test_normal_range_lower,
    )
    utils.assert_float_equal(
        result_molecular_test.blood_test_normal_range_upper,
        molecular_test.blood_test_normal_range_upper,
    )
    assert result_molecular_test.cell_count == molecular_test.cell_count
    assert result_molecular_test.chromosome == molecular_test.chromosome
    assert result_molecular_test.clonality == molecular_test.clonality
    utils.assert_float_equal(
        result_molecular_test.copy_number, molecular_test.copy_number
    )
    assert result_molecular_test.cytoband == molecular_test.cytoband
    assert result_molecular_test.days_to_test == molecular_test.days_to_test
    assert result_molecular_test.exon == molecular_test.exon
    assert result_molecular_test.gene_symbol == molecular_test.gene_symbol
    assert result_molecular_test.histone_family == molecular_test.histone_family
    assert result_molecular_test.histone_variant == molecular_test.histone_variant
    assert result_molecular_test.intron == molecular_test.intron
    assert result_molecular_test.laboratory_test == molecular_test.laboratory_test
    assert (
        result_molecular_test.loci_abnormal_count == molecular_test.loci_abnormal_count
    )
    assert result_molecular_test.loci_count == molecular_test.loci_count
    assert result_molecular_test.locus == molecular_test.locus
    assert (
        result_molecular_test.mismatch_repair_mutation
        == molecular_test.mismatch_repair_mutation
    )
    utils.assert_float_equal(
        result_molecular_test.mitotic_count, molecular_test.mitotic_count
    )
    utils.assert_float_equal(
        result_molecular_test.mitotic_total_area, molecular_test.mitotic_total_area
    )
    assert (
        result_molecular_test.molecular_analysis_method
        == molecular_test.molecular_analysis_method
    )
    assert (
        result_molecular_test.molecular_consequence
        == molecular_test.molecular_consequence
    )
    assert result_molecular_test.molecular_test_id == molecular_test.molecular_test_id
    assert result_molecular_test.pathogenicity == molecular_test.pathogenicity
    assert result_molecular_test.ploidy == molecular_test.ploidy
    assert result_molecular_test.second_exon == molecular_test.second_exon
    assert result_molecular_test.second_gene_symbol == molecular_test.second_gene_symbol
    assert (
        result_molecular_test.specialized_molecular_test
        == molecular_test.specialized_molecular_test
    )
    assert result_molecular_test.submitter_id == molecular_test.submitter_id
    assert result_molecular_test.test_analyte_type == molecular_test.test_analyte_type
    assert result_molecular_test.test_result == molecular_test.test_result
    assert result_molecular_test.test_units == molecular_test.test_units
    utils.assert_float_equal(
        result_molecular_test.test_value, molecular_test.test_value
    )
    assert result_molecular_test.transcript == molecular_test.transcript
    assert result_molecular_test.variant_origin == molecular_test.variant_origin
    assert result_molecular_test.variant_type == molecular_test.variant_type
    assert result_molecular_test.zygosity == molecular_test.zygosity


def assert_program_translated(result_program: sql.Row, program: Program) -> None:
    assert result_program.dbgap_accession_number == program.dbgap_accession_number
    assert result_program.name == program.name
    assert result_program.program_id == program.program_id


def assert_project_translated(result_project: sql.Row, project: Project) -> None:
    assert result_project.dbgap_accession_number == project.dbgap_accession_number
    assert tuple(result_project.disease_type) == project.disease_type
    assert result_project.intended_release_date == project.intended_release_date
    assert result_project.name == project.name
    assert tuple(result_project.primary_site) == project.primary_site
    assert result_project.project_id == project.project_id

    assert_program_translated(result_project.program, project.program)


def assert_follow_ups_translated(
    result_follow_ups: Iterable[sql.Row], follow_ups: Iterable[FollowUp]
) -> None:
    result_follow_up = more_itertools.one(result_follow_ups)
    follow_up = more_itertools.one(follow_ups)

    assert result_follow_up.adverse_event == follow_up.adverse_event
    assert result_follow_up.adverse_event_grade == follow_up.adverse_event_grade
    assert result_follow_up.aids_risk_factors == follow_up.aids_risk_factors
    assert (
        result_follow_up.barretts_esophagus_goblet_cells_present
        == follow_up.barretts_esophagus_goblet_cells_present
    )
    utils.assert_float_equal(result_follow_up.bmi, follow_up.bmi)
    utils.assert_float_equal(
        result_follow_up.body_surface_area, follow_up.body_surface_area
    )
    assert result_follow_up.cause_of_response == follow_up.cause_of_response
    utils.assert_float_equal(result_follow_up.cd4_count, follow_up.cd4_count)
    assert result_follow_up.cdc_hiv_risk_factors == follow_up.cdc_hiv_risk_factors
    assert result_follow_up.comorbidity == follow_up.comorbidity
    assert (
        result_follow_up.comorbidity_method_of_diagnosis
        == follow_up.comorbidity_method_of_diagnosis
    )
    assert result_follow_up.days_to_adverse_event == follow_up.days_to_adverse_event
    assert result_follow_up.days_to_comorbidity == follow_up.days_to_comorbidity
    assert result_follow_up.days_to_follow_up == follow_up.days_to_follow_up
    assert result_follow_up.days_to_imaging == follow_up.days_to_imaging
    assert result_follow_up.days_to_progression == follow_up.days_to_progression
    assert (
        result_follow_up.days_to_progression_free == follow_up.days_to_progression_free
    )
    assert result_follow_up.days_to_recurrence == follow_up.days_to_recurrence
    assert result_follow_up.diabetes_treatment_type == follow_up.diabetes_treatment_type
    assert result_follow_up.disease_response == follow_up.disease_response
    utils.assert_float_equal(
        result_follow_up.dlco_ref_predictive_percent,
        follow_up.dlco_ref_predictive_percent,
    )
    assert result_follow_up.ecog_performance_status == follow_up.ecog_performance_status
    assert (
        result_follow_up.evidence_of_recurrence_type
        == follow_up.evidence_of_recurrence_type
    )
    assert result_follow_up.eye_color == follow_up.eye_color
    utils.assert_float_equal(
        result_follow_up.fev1_fvc_post_bronch_percent,
        follow_up.fev1_fvc_post_bronch_percent,
    )
    utils.assert_float_equal(
        result_follow_up.fev1_fvc_pre_bronch_percent,
        follow_up.fev1_fvc_pre_bronch_percent,
    )
    utils.assert_float_equal(
        result_follow_up.fev1_ref_post_bronch_percent,
        follow_up.fev1_ref_post_bronch_percent,
    )
    utils.assert_float_equal(
        result_follow_up.fev1_ref_pre_bronch_percent,
        follow_up.fev1_ref_pre_bronch_percent,
    )
    assert result_follow_up.follow_up_id == follow_up.follow_up_id
    assert (
        result_follow_up.haart_treatment_indicator
        == follow_up.haart_treatment_indicator
    )
    utils.assert_float_equal(result_follow_up.height, follow_up.height)
    assert (
        result_follow_up.hepatitis_sustained_virological_response
        == follow_up.hepatitis_sustained_virological_response
    )
    assert result_follow_up.history_of_tumor == follow_up.history_of_tumor
    assert result_follow_up.history_of_tumor_type == follow_up.history_of_tumor_type
    utils.assert_float_equal(result_follow_up.hiv_viral_load, follow_up.hiv_viral_load)
    assert (
        result_follow_up.hormonal_contraceptive_type
        == follow_up.hormonal_contraceptive_type
    )
    assert (
        result_follow_up.hormonal_contraceptive_use
        == follow_up.hormonal_contraceptive_use
    )
    assert (
        result_follow_up.hormone_replacement_therapy_type
        == follow_up.hormone_replacement_therapy_type
    )
    assert result_follow_up.hpv_positive_type == follow_up.hpv_positive_type
    assert (
        result_follow_up.hysterectomy_margins_involved
        == follow_up.hysterectomy_margins_involved
    )
    assert result_follow_up.hysterectomy_type == follow_up.hysterectomy_type
    assert result_follow_up.imaging_result == follow_up.imaging_result
    assert result_follow_up.imaging_type == follow_up.imaging_type
    assert (
        result_follow_up.immunosuppressive_treatment_type
        == follow_up.immunosuppressive_treatment_type
    )
    assert (
        result_follow_up.karnofsky_performance_status
        == follow_up.karnofsky_performance_status
    )
    assert result_follow_up.menopause_status == follow_up.menopause_status
    utils.assert_float_equal(
        result_follow_up.nadir_cd4_count, follow_up.nadir_cd4_count
    )
    assert result_follow_up.pancreatitis_onset_year == follow_up.pancreatitis_onset_year
    assert result_follow_up.pregnancy_outcome == follow_up.pregnancy_outcome
    assert result_follow_up.procedures_performed == follow_up.procedures_performed
    assert (
        result_follow_up.progression_or_recurrence
        == follow_up.progression_or_recurrence
    )
    assert (
        result_follow_up.progression_or_recurrence_anatomic_site
        == follow_up.progression_or_recurrence_anatomic_site
    )
    assert (
        result_follow_up.progression_or_recurrence_type
        == follow_up.progression_or_recurrence_type
    )
    assert (
        result_follow_up.recist_targeted_regions_number
        == follow_up.recist_targeted_regions_number
    )
    utils.assert_float_equal(
        result_follow_up.recist_targeted_regions_sum,
        follow_up.recist_targeted_regions_sum,
    )
    assert result_follow_up.reflux_treatment_type == follow_up.reflux_treatment_type
    assert result_follow_up.risk_factor == follow_up.risk_factor
    assert result_follow_up.risk_factor_treatment == follow_up.risk_factor_treatment
    assert result_follow_up.scan_tracer_used == follow_up.scan_tracer_used
    assert result_follow_up.state == follow_up.state
    assert result_follow_up.submitter_id == follow_up.submitter_id
    assert (
        result_follow_up.undescended_testis_corrected
        == follow_up.undescended_testis_corrected
    )
    assert (
        result_follow_up.undescended_testis_corrected_age
        == follow_up.undescended_testis_corrected_age
    )
    assert (
        result_follow_up.undescended_testis_corrected_laterality
        == follow_up.undescended_testis_corrected_laterality
    )
    assert (
        result_follow_up.undescended_testis_corrected_method
        == follow_up.undescended_testis_corrected_method
    )
    assert (
        result_follow_up.undescended_testis_history
        == follow_up.undescended_testis_history
    )
    assert (
        result_follow_up.undescended_testis_history_laterality
        == follow_up.undescended_testis_history_laterality
    )
    assert (
        result_follow_up.viral_hepatitis_serologies
        == follow_up.viral_hepatitis_serologies
    )
    utils.assert_float_equal(result_follow_up.weight, follow_up.weight)

    assert_molecular_tests_translated(
        result_follow_up.molecular_tests, follow_up.molecular_tests
    )


def assert_data_categoryies_translated(
    result_data_categories: Iterable[sql.Row], data_categories: Iterable[DataCategory]
) -> None:
    result_data_category = more_itertools.one(result_data_categories)
    data_category = more_itertools.one(data_categories)

    assert result_data_category.data_category == data_category.data_category
    assert result_data_category.file_count == data_category.file_count


def assert_experimental_strategies_translated(
    result_experimental_strategies: Iterable[sql.Row],
    experimental_strategies: Iterable[ExperimentalStrategy],
) -> None:
    result_experimental_strategy = more_itertools.one(result_experimental_strategies)
    experimental_strategy = more_itertools.one(experimental_strategies)

    assert (
        result_experimental_strategy.experimental_strategy
        == experimental_strategy.experimental_strategy
    )
    assert result_experimental_strategy.file_count == experimental_strategy.file_count


def assert_summary_translated(result_summary: sql.Row, summary: Summary) -> None:
    assert result_summary.file_count == summary.file_count
    assert result_summary.file_size == summary.file_size

    assert_data_categoryies_translated(
        result_summary.data_categories, summary.data_categories
    )
    assert_experimental_strategies_translated(
        result_summary.experimental_strategies, summary.experimental_strategies
    )


def assert_tissue_source_stie_translated(
    result_tissue_source_site: sql.Row, tissue_source_site: TissueSourceSite
) -> None:
    assert result_tissue_source_site.bcr_id == tissue_source_site.bcr_id
    assert result_tissue_source_site.code == tissue_source_site.code
    assert result_tissue_source_site.name == tissue_source_site.name
    assert result_tissue_source_site.project == tissue_source_site.project
    assert (
        result_tissue_source_site.tissue_source_site_id
        == tissue_source_site.tissue_source_site_id
    )


def assert_case_translated(result_case: sql.Row, case: Case) -> None:
    assert result_case.case_id == case.case_id
    assert result_case.consent_type == case.consent_type
    assert result_case.days_to_consent == case.days_to_consent
    assert result_case.days_to_lost_to_followup == case.days_to_lost_to_followup
    assert result_case.disease_type == case.disease_type
    assert result_case.index_date == case.index_date
    assert result_case.lost_to_followup == case.lost_to_followup
    assert result_case.primary_site == case.primary_site
    assert result_case.state == case.state
    assert result_case.submitter_id == case.submitter_id

    assert_demographic_translated(result_case.demographic, case.demographic)
    assert_diagnoses_translated(result_case.diagnoses, case.diagnoses)
    assert_exposures_translated(result_case.exposures, case.exposures)
    assert_family_histories_translated(
        result_case.family_histories, case.family_histories
    )
    assert_follow_ups_translated(result_case.follow_ups, case.follow_ups)
    assert_project_translated(result_case.project, case.project)
    assert_tissue_source_stie_translated(
        result_case.tissue_source_site, case.tissue_source_site
    )
