import dataclasses
import random
import string
import sys
from typing import Dict, FrozenSet, Iterable, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from exports import builders
from exports.configuration.builders import common, viz
from exports.constants import build
from tests.unit.data import schemas

CASE_ID_SCHEMA = "case_id: string"


def random_string() -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(10))


def random_integer() -> int:
    return random.randint(0, sys.maxsize)


def random_float() -> float:
    return random.random() + random.randint(0, 100_000_000_000)


@dataclasses.dataclass(frozen=True)
class Demographic:
    age_at_index: int = dataclasses.field(default_factory=random_integer)
    age_is_obfuscated: str = dataclasses.field(default_factory=random_string)
    cause_of_death: str = dataclasses.field(default_factory=random_string)
    cause_of_death_source: str = dataclasses.field(default_factory=random_string)
    country_of_residence_at_enrollment: str = dataclasses.field(
        default_factory=random_string
    )
    days_to_birth: int = dataclasses.field(default_factory=random_integer)
    days_to_death: int = dataclasses.field(default_factory=random_integer)
    demographic_id: str = dataclasses.field(default_factory=random_string)
    ethnicity: str = dataclasses.field(default_factory=random_string)
    gender: str = dataclasses.field(default_factory=random_string)
    occupation_duration_years: int = dataclasses.field(default_factory=random_integer)
    premature_at_birth: str = dataclasses.field(default_factory=random_string)
    race: str = dataclasses.field(default_factory=random_string)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    vital_status: str = dataclasses.field(default_factory=random_string)
    weeks_gestation_at_birth: float = dataclasses.field(default_factory=random_float)
    year_of_birth: int = dataclasses.field(default_factory=random_integer)
    year_of_death: int = dataclasses.field(default_factory=random_integer)


@dataclasses.dataclass(frozen=True)
class PathologyDetail:
    additional_pathology_findings: str = dataclasses.field(
        default_factory=random_string
    )
    anaplasia_present: str = dataclasses.field(default_factory=random_string)
    anaplasia_present_type: str = dataclasses.field(default_factory=random_string)
    bone_marrow_malignant_cells: str = dataclasses.field(default_factory=random_string)
    breslow_thickness: float = dataclasses.field(default_factory=random_float)
    circumferential_resection_margin: float = dataclasses.field(
        default_factory=random_float
    )
    columnar_mucosa_present: str = dataclasses.field(default_factory=random_string)
    consistent_pathology_review: str = dataclasses.field(default_factory=random_string)
    dysplasia_degree: str = dataclasses.field(default_factory=random_string)
    dysplasia_type: str = dataclasses.field(default_factory=random_string)
    greatest_tumor_dimension: float = dataclasses.field(default_factory=random_float)
    gross_tumor_weight: float = dataclasses.field(default_factory=random_float)
    largest_extrapelvic_peritoneal_focus: str = dataclasses.field(
        default_factory=random_string
    )
    lymph_node_involved_site: str = dataclasses.field(default_factory=random_string)
    lymph_node_involvement: str = dataclasses.field(default_factory=random_string)
    lymph_nodes_positive: int = dataclasses.field(default_factory=random_integer)
    lymph_nodes_tested: int = dataclasses.field(default_factory=random_integer)
    lymphatic_invasion_present: str = dataclasses.field(default_factory=random_string)
    margin_status: str = dataclasses.field(default_factory=random_string)
    metaplasia_present: str = dataclasses.field(default_factory=random_string)
    morphologic_architectural_pattern: str = dataclasses.field(
        default_factory=random_string
    )
    necrosis_percent: float = dataclasses.field(default_factory=random_float)
    non_nodal_regional_disease: str = dataclasses.field(default_factory=random_string)
    non_nodal_tumor_deposits: str = dataclasses.field(default_factory=random_string)
    number_proliferating_cells: int = dataclasses.field(default_factory=random_integer)
    pathology_detail_id: str = dataclasses.field(default_factory=random_string)
    percent_tumor_invasion: float = dataclasses.field(default_factory=random_float)
    perineural_invasion_present: str = dataclasses.field(default_factory=random_string)
    peripancreatic_lymph_nodes_positive: str = dataclasses.field(
        default_factory=random_string
    )
    peripancreatic_lymph_nodes_tested: int = dataclasses.field(
        default_factory=random_integer
    )
    prostatic_chips_positive_count: float = dataclasses.field(
        default_factory=random_float
    )
    prostatic_chips_total_count: float = dataclasses.field(default_factory=random_float)
    prostatic_involvement_percent: float = dataclasses.field(
        default_factory=random_float
    )
    residual_tumor: str = dataclasses.field(default_factory=random_string)
    rhabdoid_percent: float = dataclasses.field(default_factory=random_float)
    rhabdoid_present: str = dataclasses.field(default_factory=random_string)
    sarcomatoid_percent: float = dataclasses.field(default_factory=random_float)
    sarcomatoid_present: str = dataclasses.field(default_factory=random_string)
    size_extraocular_nodule: float = dataclasses.field(default_factory=random_float)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    transglottic_extension: str = dataclasses.field(default_factory=random_string)
    tumor_largest_dimension_diameter: float = dataclasses.field(
        default_factory=random_float
    )
    tumor_thickness: float = dataclasses.field(default_factory=random_float)
    vascular_invasion_present: str = dataclasses.field(default_factory=random_string)
    vascular_invasion_type: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class Treatment:
    chemo_concurrent_to_radiation: str = dataclasses.field(
        default_factory=random_string
    )
    days_to_treatment_end: int = dataclasses.field(default_factory=random_integer)
    days_to_treatment_start: int = dataclasses.field(default_factory=random_integer)
    initial_disease_status: str = dataclasses.field(default_factory=random_string)
    number_of_cycles: int = dataclasses.field(default_factory=random_integer)
    reason_treatment_ended: str = dataclasses.field(default_factory=random_string)
    regimen_or_line_of_therapy: str = dataclasses.field(default_factory=random_string)
    route_of_administration: str = dataclasses.field(default_factory=random_string)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    therapeutic_agents: str = dataclasses.field(default_factory=random_string)
    treatment_anatomic_site: str = dataclasses.field(default_factory=random_string)
    treatment_arm: str = dataclasses.field(default_factory=random_string)
    treatment_dose: int = dataclasses.field(default_factory=random_integer)
    treatment_dose_units: str = dataclasses.field(default_factory=random_string)
    treatment_effect: str = dataclasses.field(default_factory=random_string)
    treatment_effect_indicator: str = dataclasses.field(default_factory=random_string)
    treatment_frequency: str = dataclasses.field(default_factory=random_string)
    treatment_id: str = dataclasses.field(default_factory=random_string)
    treatment_intent_type: str = dataclasses.field(default_factory=random_string)
    treatment_or_therapy: str = dataclasses.field(default_factory=random_string)
    treatment_outcome: str = dataclasses.field(default_factory=random_string)
    treatment_type: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class Diagnosis:
    adrenal_hormone: str = dataclasses.field(default_factory=random_string)
    age_at_diagnosis: int = dataclasses.field(default_factory=random_integer)
    ajcc_clinical_m: str = dataclasses.field(default_factory=random_string)
    ajcc_clinical_n: str = dataclasses.field(default_factory=random_string)
    ajcc_clinical_stage: str = dataclasses.field(default_factory=random_string)
    ajcc_clinical_t: str = dataclasses.field(default_factory=random_string)
    ajcc_pathologic_m: str = dataclasses.field(default_factory=random_string)
    ajcc_pathologic_n: str = dataclasses.field(default_factory=random_string)
    ajcc_pathologic_stage: str = dataclasses.field(default_factory=random_string)
    ajcc_pathologic_t: str = dataclasses.field(default_factory=random_string)
    ajcc_staging_system_edition: str = dataclasses.field(default_factory=random_string)
    ann_arbor_b_symptoms: str = dataclasses.field(default_factory=random_string)
    ann_arbor_b_symptoms_described: str = dataclasses.field(
        default_factory=random_string
    )
    ann_arbor_clinical_stage: str = dataclasses.field(default_factory=random_string)
    ann_arbor_extranodal_involvement: str = dataclasses.field(
        default_factory=random_string
    )
    ann_arbor_pathologic_stage: str = dataclasses.field(default_factory=random_string)
    burkitt_lymphoma_clinical_variant: str = dataclasses.field(
        default_factory=random_string
    )
    child_pugh_classification: str = dataclasses.field(default_factory=random_string)
    classification_of_tumor: str = dataclasses.field(default_factory=random_string)
    cog_liver_stage: str = dataclasses.field(default_factory=random_string)
    cog_neuroblastoma_risk_group: str = dataclasses.field(default_factory=random_string)
    cog_renal_stage: str = dataclasses.field(default_factory=random_string)
    days_to_diagnosis: int = dataclasses.field(default_factory=random_integer)
    days_to_last_follow_up: float = dataclasses.field(default_factory=random_float)
    days_to_last_known_disease_status: float = dataclasses.field(
        default_factory=random_float
    )
    days_to_recurrence: float = dataclasses.field(default_factory=random_float)
    diagnosis_id: str = dataclasses.field(default_factory=random_string)
    enneking_msts_grade: str = dataclasses.field(default_factory=random_string)
    enneking_msts_metastasis: str = dataclasses.field(default_factory=random_string)
    enneking_msts_stage: str = dataclasses.field(default_factory=random_string)
    enneking_msts_tumor_site: str = dataclasses.field(default_factory=random_string)
    esophageal_columnar_dysplasia_degree: str = dataclasses.field(
        default_factory=random_string
    )
    esophageal_columnar_metaplasia_present: str = dataclasses.field(
        default_factory=random_string
    )
    figo_stage: str = dataclasses.field(default_factory=random_string)
    figo_staging_edition_year: str = dataclasses.field(default_factory=random_string)
    first_symptom_prior_to_diagnosis: str = dataclasses.field(
        default_factory=random_string
    )
    gastric_esophageal_junction_involvement: str = dataclasses.field(
        default_factory=random_string
    )
    gleason_grade_group: str = dataclasses.field(default_factory=random_string)
    gleason_grade_tertiary: str = dataclasses.field(default_factory=random_string)
    gleason_patterns_percent: int = dataclasses.field(default_factory=random_integer)
    goblet_cells_columnar_mucosa_present: str = dataclasses.field(
        default_factory=random_string
    )
    icd_10_code: str = dataclasses.field(default_factory=random_string)
    igcccg_stage: str = dataclasses.field(default_factory=random_string)
    inpc_grade: str = dataclasses.field(default_factory=random_string)
    inpc_histologic_group: str = dataclasses.field(default_factory=random_string)
    inrg_stage: str = dataclasses.field(default_factory=random_string)
    inss_stage: str = dataclasses.field(default_factory=random_string)
    international_prognostic_index: str = dataclasses.field(
        default_factory=random_string
    )
    irs_stage: str = dataclasses.field(default_factory=random_string)
    ishak_fibrosis_score: str = dataclasses.field(default_factory=random_string)
    iss_stage: str = dataclasses.field(default_factory=random_string)
    last_known_disease_status: str = dataclasses.field(default_factory=random_string)
    laterality: str = dataclasses.field(default_factory=random_string)
    margin_distance: float = dataclasses.field(default_factory=random_float)
    margins_involved_site: str = dataclasses.field(default_factory=random_string)
    masaoka_stage: str = dataclasses.field(default_factory=random_string)
    medulloblastoma_molecular_classification: str = dataclasses.field(
        default_factory=random_string
    )
    metastasis_at_diagnosis: str = dataclasses.field(default_factory=random_string)
    metastasis_at_diagnosis_site: str = dataclasses.field(default_factory=random_string)
    method_of_diagnosis: str = dataclasses.field(default_factory=random_string)
    micropapillary_features: str = dataclasses.field(default_factory=random_string)
    mitosis_karyorrhexis_index: str = dataclasses.field(default_factory=random_string)
    mitotic_count: int = dataclasses.field(default_factory=random_integer)
    morphology: str = dataclasses.field(default_factory=random_string)
    ovarian_specimen_status: str = dataclasses.field(default_factory=random_string)
    ovarian_surface_involvement: str = dataclasses.field(default_factory=random_string)
    papillary_renal_cell_type: str = dataclasses.field(default_factory=random_string)
    pathology_details: Iterable[PathologyDetail] = (PathologyDetail(),)
    peritoneal_fluid_cytological_status: str = dataclasses.field(
        default_factory=random_string
    )
    pregnant_at_diagnosis: str = dataclasses.field(default_factory=random_string)
    primary_diagnosis: str = dataclasses.field(default_factory=random_string)
    primary_disease: str = dataclasses.field(default_factory=random_string)
    primary_gleason_grade: str = dataclasses.field(default_factory=random_string)
    prior_malignancy: str = dataclasses.field(default_factory=random_string)
    prior_treatment: str = dataclasses.field(default_factory=random_string)
    progression_or_recurrence: str = dataclasses.field(default_factory=random_string)
    residual_disease: str = dataclasses.field(default_factory=random_string)
    secondary_gleason_grade: str = dataclasses.field(default_factory=random_string)
    site_of_resection_or_biopsy: str = dataclasses.field(default_factory=random_string)
    sites_of_involvement: str = dataclasses.field(default_factory=random_string)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    supratentorial_localization: str = dataclasses.field(default_factory=random_string)
    synchronous_malignancy: str = dataclasses.field(default_factory=random_string)
    tissue_or_organ_of_origin: str = dataclasses.field(default_factory=random_string)
    treatments: Iterable[Treatment] = (Treatment(),)
    tumor_depth: float = dataclasses.field(default_factory=random_float)
    tumor_grade: str = dataclasses.field(default_factory=random_string)
    tumor_regression_grade: str = dataclasses.field(default_factory=random_string)
    weiss_assessment_score: str = dataclasses.field(default_factory=random_string)
    who_cns_grade: str = dataclasses.field(default_factory=random_string)
    who_nte_grade: str = dataclasses.field(default_factory=random_string)
    year_of_diagnosis: int = dataclasses.field(default_factory=random_integer)


@dataclasses.dataclass(frozen=True)
class Exposure:
    age_at_onset: int = dataclasses.field(default_factory=random_integer)
    alcohol_days_per_week: float = dataclasses.field(default_factory=random_float)
    alcohol_history: str = dataclasses.field(default_factory=random_string)
    alcohol_intensity: str = dataclasses.field(default_factory=random_string)
    alcohol_type: str = dataclasses.field(default_factory=random_string)
    asbestos_exposure: str = dataclasses.field(default_factory=random_string)
    cigarettes_per_day: float = dataclasses.field(default_factory=random_float)
    coal_dust_exposure: str = dataclasses.field(default_factory=random_string)
    environmental_tobacco_smoke_exposure: str = dataclasses.field(
        default_factory=random_string
    )
    exposure_duration: str = dataclasses.field(default_factory=random_string)
    exposure_duration_years: int = dataclasses.field(default_factory=random_integer)
    exposure_id: str = dataclasses.field(default_factory=random_string)
    marijuana_use_per_week: float = dataclasses.field(default_factory=random_float)
    pack_years_smoked: float = dataclasses.field(default_factory=random_float)
    radon_exposure: str = dataclasses.field(default_factory=random_string)
    respirable_crystalline_silica_exposure: str = dataclasses.field(
        default_factory=random_string
    )
    smokeless_tobacco_quit_age: int = dataclasses.field(default_factory=random_integer)
    smoking_frequency: str = dataclasses.field(default_factory=random_string)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    time_between_waking_and_first_smoke: str = dataclasses.field(
        default_factory=random_string
    )
    tobacco_smoking_onset_year: int = dataclasses.field(default_factory=random_integer)
    tobacco_smoking_quit_year: int = dataclasses.field(default_factory=random_integer)
    tobacco_smoking_status: str = dataclasses.field(default_factory=random_string)
    tobacco_use_per_day: float = dataclasses.field(default_factory=random_float)
    years_smoked: float = dataclasses.field(default_factory=random_float)


@dataclasses.dataclass(frozen=True)
class Program:
    dbgap_accession_number: str = dataclasses.field(default_factory=random_string)
    name: str = dataclasses.field(default_factory=random_string)
    program_id: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class Project:
    dbgap_accession_number: str = dataclasses.field(default_factory=random_string)
    intended_release_date: str = dataclasses.field(default_factory=random_string)
    name: str = dataclasses.field(default_factory=random_string)
    program: Program = Program()
    project_id: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class Sample:
    sample_type: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class DataCategory:
    data_category: str = dataclasses.field(default_factory=random_string)
    file_count: int = dataclasses.field(default_factory=random_integer)


@dataclasses.dataclass(frozen=True)
class ExperimentalStrategy:
    experimental_strategy: str = dataclasses.field(default_factory=random_string)
    file_count: int = dataclasses.field(default_factory=random_integer)


@dataclasses.dataclass(frozen=True)
class Summary:
    data_categories: Iterable[DataCategory] = (DataCategory(),)
    experimental_strategies: Iterable[ExperimentalStrategy] = (ExperimentalStrategy(),)
    file_count: int = dataclasses.field(default_factory=random_integer)
    file_size: int = dataclasses.field(default_factory=random_integer)


@dataclasses.dataclass(frozen=True)
class TissueSourceSite:
    bcr_id: str = dataclasses.field(default_factory=random_string)
    code: str = dataclasses.field(default_factory=random_string)
    name: str = dataclasses.field(default_factory=random_string)
    project: str = dataclasses.field(default_factory=random_string)
    tissue_source_site_id: str = dataclasses.field(default_factory=random_string)


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: str = dataclasses.field(default_factory=random_string)
    consent_type: str = dataclasses.field(default_factory=random_string)
    created_datetime: str = dataclasses.field(default_factory=random_string)
    days_to_consent: int = dataclasses.field(default_factory=random_integer)
    days_to_lost_to_followup: int = dataclasses.field(default_factory=random_integer)
    demographic: Demographic = Demographic()
    diagnoses: Iterable[Diagnosis] = (Diagnosis(),)
    disease_type: str = dataclasses.field(default_factory=random_string)
    exposures: Iterable[Exposure] = (Exposure(),)
    index_date: str = dataclasses.field(default_factory=random_string)
    lost_to_followup: str = dataclasses.field(default_factory=random_string)
    primary_site: str = dataclasses.field(default_factory=random_string)
    project: Project = Project()
    samples: Iterable[Sample] = (Sample(),)
    state: str = dataclasses.field(default_factory=random_string)
    submitter_id: str = dataclasses.field(default_factory=random_string)
    summary: Summary = Summary()
    tissue_source_site: TissueSourceSite = TissueSourceSite()
    updated_datetime: str = dataclasses.field(default_factory=random_string)


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.load_schema("builders/case/input_case.yaml")


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.load_schema("builders/case/final_case.yaml")


def assert_demographics_equal(
    result_demographic: sql.Row, demographic: Demographic
) -> None:
    assert result_demographic.age_at_index == demographic.age_at_index
    assert result_demographic.age_is_obfuscated == demographic.age_is_obfuscated
    assert result_demographic.cause_of_death == demographic.cause_of_death
    assert result_demographic.cause_of_death_source == demographic.cause_of_death_source
    assert (
        result_demographic.country_of_residence_at_enrollment
        == demographic.country_of_residence_at_enrollment
    )
    assert result_demographic.days_to_birth == demographic.days_to_birth
    assert result_demographic.days_to_death == demographic.days_to_death
    assert result_demographic.demographic_id == demographic.demographic_id
    assert result_demographic.ethnicity == demographic.ethnicity
    assert result_demographic.gender == demographic.gender
    assert (
        result_demographic.occupation_duration_years
        == demographic.occupation_duration_years
    )
    assert result_demographic.premature_at_birth == demographic.premature_at_birth
    assert result_demographic.race == demographic.race
    assert result_demographic.state == demographic.state
    assert result_demographic.submitter_id == demographic.submitter_id
    assert result_demographic.vital_status == demographic.vital_status
    assert (
        result_demographic.weeks_gestation_at_birth
        == demographic.weeks_gestation_at_birth
    )
    assert result_demographic.year_of_birth == demographic.year_of_birth
    assert result_demographic.year_of_death == demographic.year_of_death


def assert_pathology_details_equal(
    result_pathology_detail: sql.Row, pathology_detail: PathologyDetail
) -> None:
    assert (
        result_pathology_detail.additional_pathology_findings
        == pathology_detail.additional_pathology_findings
    )
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
    assert (
        result_pathology_detail.breslow_thickness == pathology_detail.breslow_thickness
    )
    assert (
        result_pathology_detail.circumferential_resection_margin
        == pathology_detail.circumferential_resection_margin
    )
    assert (
        result_pathology_detail.columnar_mucosa_present
        == pathology_detail.columnar_mucosa_present
    )
    assert (
        result_pathology_detail.consistent_pathology_review
        == pathology_detail.consistent_pathology_review
    )
    assert result_pathology_detail.dysplasia_degree == pathology_detail.dysplasia_degree
    assert result_pathology_detail.dysplasia_type == pathology_detail.dysplasia_type
    assert (
        result_pathology_detail.greatest_tumor_dimension
        == pathology_detail.greatest_tumor_dimension
    )
    assert (
        result_pathology_detail.gross_tumor_weight
        == pathology_detail.gross_tumor_weight
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
    assert result_pathology_detail.necrosis_percent == pathology_detail.necrosis_percent
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
    assert (
        result_pathology_detail.percent_tumor_invasion
        == pathology_detail.percent_tumor_invasion
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
    assert (
        result_pathology_detail.prostatic_chips_positive_count
        == pathology_detail.prostatic_chips_positive_count
    )
    assert (
        result_pathology_detail.prostatic_chips_total_count
        == pathology_detail.prostatic_chips_total_count
    )
    assert (
        result_pathology_detail.prostatic_involvement_percent
        == pathology_detail.prostatic_involvement_percent
    )
    assert result_pathology_detail.residual_tumor == pathology_detail.residual_tumor
    assert result_pathology_detail.rhabdoid_percent == pathology_detail.rhabdoid_percent
    assert result_pathology_detail.rhabdoid_present == pathology_detail.rhabdoid_present
    assert (
        result_pathology_detail.sarcomatoid_percent
        == pathology_detail.sarcomatoid_percent
    )
    assert (
        result_pathology_detail.sarcomatoid_present
        == pathology_detail.sarcomatoid_present
    )
    assert (
        result_pathology_detail.size_extraocular_nodule
        == pathology_detail.size_extraocular_nodule
    )
    assert result_pathology_detail.state == pathology_detail.state
    assert result_pathology_detail.submitter_id == pathology_detail.submitter_id
    assert (
        result_pathology_detail.transglottic_extension
        == pathology_detail.transglottic_extension
    )
    assert (
        result_pathology_detail.tumor_largest_dimension_diameter
        == pathology_detail.tumor_largest_dimension_diameter
    )
    assert result_pathology_detail.tumor_thickness == pathology_detail.tumor_thickness
    assert (
        result_pathology_detail.vascular_invasion_present
        == pathology_detail.vascular_invasion_present
    )
    assert (
        result_pathology_detail.vascular_invasion_type
        == pathology_detail.vascular_invasion_type
    )


def assert_treatments_equal(result_treatment: sql.Row, treatment: Treatment) -> None:
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
    assert result_treatment.route_of_administration == treatment.route_of_administration
    assert result_treatment.state == treatment.state
    assert result_treatment.submitter_id == treatment.submitter_id
    assert result_treatment.therapeutic_agents == treatment.therapeutic_agents
    assert result_treatment.treatment_anatomic_site == treatment.treatment_anatomic_site
    assert result_treatment.treatment_arm == treatment.treatment_arm
    assert result_treatment.treatment_dose == treatment.treatment_dose
    assert result_treatment.treatment_dose_units == treatment.treatment_dose_units
    assert result_treatment.treatment_effect == treatment.treatment_effect
    assert (
        result_treatment.treatment_effect_indicator
        == treatment.treatment_effect_indicator
    )
    assert result_treatment.treatment_frequency == treatment.treatment_frequency
    assert result_treatment.treatment_id == treatment.treatment_id
    assert result_treatment.treatment_intent_type == treatment.treatment_intent_type
    assert result_treatment.treatment_or_therapy == treatment.treatment_or_therapy
    assert result_treatment.treatment_outcome == treatment.treatment_outcome
    assert result_treatment.treatment_type == treatment.treatment_type


def assert_diagnoses_equal(result_diagnosis: sql.Row, diagnosis: Diagnosis) -> None:
    assert result_diagnosis.adrenal_hormone == diagnosis.adrenal_hormone
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
        result_diagnosis.ann_arbor_b_symptoms_described
        == diagnosis.ann_arbor_b_symptoms_described
    )
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
    assert (
        result_diagnosis.burkitt_lymphoma_clinical_variant
        == diagnosis.burkitt_lymphoma_clinical_variant
    )
    assert (
        result_diagnosis.child_pugh_classification
        == diagnosis.child_pugh_classification
    )
    assert result_diagnosis.classification_of_tumor == diagnosis.classification_of_tumor
    assert result_diagnosis.cog_liver_stage == diagnosis.cog_liver_stage
    assert (
        result_diagnosis.cog_neuroblastoma_risk_group
        == diagnosis.cog_neuroblastoma_risk_group
    )
    assert result_diagnosis.cog_renal_stage == diagnosis.cog_renal_stage
    assert result_diagnosis.days_to_diagnosis == diagnosis.days_to_diagnosis
    assert result_diagnosis.days_to_last_follow_up == diagnosis.days_to_last_follow_up
    assert (
        result_diagnosis.days_to_last_known_disease_status
        == diagnosis.days_to_last_known_disease_status
    )
    assert result_diagnosis.days_to_recurrence == diagnosis.days_to_recurrence
    assert result_diagnosis.diagnosis_id == diagnosis.diagnosis_id
    assert result_diagnosis.enneking_msts_grade == diagnosis.enneking_msts_grade
    assert (
        result_diagnosis.enneking_msts_metastasis == diagnosis.enneking_msts_metastasis
    )
    assert result_diagnosis.enneking_msts_stage == diagnosis.enneking_msts_stage
    assert (
        result_diagnosis.enneking_msts_tumor_site == diagnosis.enneking_msts_tumor_site
    )
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
        result_diagnosis.first_symptom_prior_to_diagnosis
        == diagnosis.first_symptom_prior_to_diagnosis
    )
    assert (
        result_diagnosis.gastric_esophageal_junction_involvement
        == diagnosis.gastric_esophageal_junction_involvement
    )
    assert result_diagnosis.gleason_grade_group == diagnosis.gleason_grade_group
    assert result_diagnosis.gleason_grade_tertiary == diagnosis.gleason_grade_tertiary
    assert (
        result_diagnosis.gleason_patterns_percent == diagnosis.gleason_patterns_percent
    )
    assert (
        result_diagnosis.goblet_cells_columnar_mucosa_present
        == diagnosis.goblet_cells_columnar_mucosa_present
    )
    assert result_diagnosis.icd_10_code == diagnosis.icd_10_code
    assert result_diagnosis.igcccg_stage == diagnosis.igcccg_stage
    assert result_diagnosis.inpc_grade == diagnosis.inpc_grade
    assert result_diagnosis.inpc_histologic_group == diagnosis.inpc_histologic_group
    assert result_diagnosis.inrg_stage == diagnosis.inrg_stage
    assert result_diagnosis.inss_stage == diagnosis.inss_stage
    assert (
        result_diagnosis.international_prognostic_index
        == diagnosis.international_prognostic_index
    )
    assert result_diagnosis.irs_stage == diagnosis.irs_stage
    assert result_diagnosis.ishak_fibrosis_score == diagnosis.ishak_fibrosis_score
    assert result_diagnosis.iss_stage == diagnosis.iss_stage
    assert (
        result_diagnosis.last_known_disease_status
        == diagnosis.last_known_disease_status
    )
    assert result_diagnosis.laterality == diagnosis.laterality
    assert result_diagnosis.margin_distance == diagnosis.margin_distance
    assert result_diagnosis.margins_involved_site == diagnosis.margins_involved_site
    assert result_diagnosis.masaoka_stage == diagnosis.masaoka_stage
    assert (
        result_diagnosis.medulloblastoma_molecular_classification
        == diagnosis.medulloblastoma_molecular_classification
    )
    assert result_diagnosis.metastasis_at_diagnosis == diagnosis.metastasis_at_diagnosis
    assert (
        result_diagnosis.metastasis_at_diagnosis_site
        == diagnosis.metastasis_at_diagnosis_site
    )
    assert result_diagnosis.method_of_diagnosis == diagnosis.method_of_diagnosis
    assert result_diagnosis.micropapillary_features == diagnosis.micropapillary_features
    assert (
        result_diagnosis.mitosis_karyorrhexis_index
        == diagnosis.mitosis_karyorrhexis_index
    )
    assert result_diagnosis.mitotic_count == diagnosis.mitotic_count
    assert result_diagnosis.morphology == diagnosis.morphology
    assert result_diagnosis.ovarian_specimen_status == diagnosis.ovarian_specimen_status
    assert (
        result_diagnosis.ovarian_surface_involvement
        == diagnosis.ovarian_surface_involvement
    )
    assert (
        result_diagnosis.papillary_renal_cell_type
        == diagnosis.papillary_renal_cell_type
    )
    assert (
        result_diagnosis.peritoneal_fluid_cytological_status
        == diagnosis.peritoneal_fluid_cytological_status
    )
    assert result_diagnosis.pregnant_at_diagnosis == diagnosis.pregnant_at_diagnosis
    assert result_diagnosis.primary_diagnosis == diagnosis.primary_diagnosis
    assert result_diagnosis.primary_disease == diagnosis.primary_disease
    assert result_diagnosis.primary_gleason_grade == diagnosis.primary_gleason_grade
    assert result_diagnosis.prior_malignancy == diagnosis.prior_malignancy
    assert result_diagnosis.prior_treatment == diagnosis.prior_treatment
    assert (
        result_diagnosis.progression_or_recurrence
        == diagnosis.progression_or_recurrence
    )
    assert result_diagnosis.residual_disease == diagnosis.residual_disease
    assert result_diagnosis.secondary_gleason_grade == diagnosis.secondary_gleason_grade
    assert (
        result_diagnosis.site_of_resection_or_biopsy
        == diagnosis.site_of_resection_or_biopsy
    )
    assert result_diagnosis.sites_of_involvement == diagnosis.sites_of_involvement
    assert result_diagnosis.state == diagnosis.state
    assert result_diagnosis.submitter_id == diagnosis.submitter_id
    assert (
        result_diagnosis.supratentorial_localization
        == diagnosis.supratentorial_localization
    )
    assert result_diagnosis.synchronous_malignancy == diagnosis.synchronous_malignancy
    assert (
        result_diagnosis.tissue_or_organ_of_origin
        == diagnosis.tissue_or_organ_of_origin
    )
    assert result_diagnosis.tumor_depth == diagnosis.tumor_depth
    assert result_diagnosis.tumor_grade == diagnosis.tumor_grade
    assert result_diagnosis.tumor_regression_grade == diagnosis.tumor_regression_grade
    assert result_diagnosis.weiss_assessment_score == diagnosis.weiss_assessment_score
    assert result_diagnosis.who_cns_grade == diagnosis.who_cns_grade
    assert result_diagnosis.who_nte_grade == diagnosis.who_nte_grade
    assert result_diagnosis.year_of_diagnosis == diagnosis.year_of_diagnosis

    for result_pathology_detail, pathology_detail in more_itertools.zip_equal(
        result_diagnosis.pathology_details, diagnosis.pathology_details
    ):
        assert_pathology_details_equal(result_pathology_detail, pathology_detail)

    for result_treatment, treatment in more_itertools.zip_equal(
        result_diagnosis.treatments, diagnosis.treatments
    ):
        assert_treatments_equal(result_treatment, treatment)


def assert_exposures_equal(result_exposure: sql.Row, exposure: Exposure) -> None:
    assert result_exposure.age_at_onset == exposure.age_at_onset
    assert result_exposure.alcohol_days_per_week == exposure.alcohol_days_per_week
    assert result_exposure.alcohol_history == exposure.alcohol_history
    assert result_exposure.alcohol_intensity == exposure.alcohol_intensity
    assert result_exposure.alcohol_type == exposure.alcohol_type
    assert result_exposure.asbestos_exposure == exposure.asbestos_exposure
    assert result_exposure.cigarettes_per_day == exposure.cigarettes_per_day
    assert result_exposure.coal_dust_exposure == exposure.coal_dust_exposure
    assert (
        result_exposure.environmental_tobacco_smoke_exposure
        == exposure.environmental_tobacco_smoke_exposure
    )
    assert result_exposure.exposure_duration == exposure.exposure_duration
    assert result_exposure.exposure_duration_years == exposure.exposure_duration_years
    assert result_exposure.exposure_id == exposure.exposure_id
    assert result_exposure.marijuana_use_per_week == exposure.marijuana_use_per_week
    assert result_exposure.pack_years_smoked == exposure.pack_years_smoked
    assert result_exposure.radon_exposure == exposure.radon_exposure
    assert (
        result_exposure.respirable_crystalline_silica_exposure
        == exposure.respirable_crystalline_silica_exposure
    )
    assert (
        result_exposure.smokeless_tobacco_quit_age
        == exposure.smokeless_tobacco_quit_age
    )
    assert result_exposure.smoking_frequency == exposure.smoking_frequency
    assert result_exposure.state == exposure.state
    assert result_exposure.submitter_id == exposure.submitter_id
    assert (
        result_exposure.time_between_waking_and_first_smoke
        == exposure.time_between_waking_and_first_smoke
    )
    assert (
        result_exposure.tobacco_smoking_onset_year
        == exposure.tobacco_smoking_onset_year
    )
    assert (
        result_exposure.tobacco_smoking_quit_year == exposure.tobacco_smoking_quit_year
    )
    assert result_exposure.tobacco_smoking_status == exposure.tobacco_smoking_status
    assert result_exposure.tobacco_use_per_day == exposure.tobacco_use_per_day
    assert result_exposure.years_smoked == exposure.years_smoked


def assert_programs_equal(result_program: sql.Row, program: Program) -> None:
    assert result_program.dbgap_accession_number == program.dbgap_accession_number
    assert result_program.name == program.name
    assert result_program.program_id == program.program_id


def assert_projects_equal(result_project: sql.Row, project: Project) -> None:
    assert result_project.dbgap_accession_number == project.dbgap_accession_number
    assert result_project.intended_release_date == project.intended_release_date
    assert result_project.name == project.name
    assert result_project.project_id == project.project_id

    assert_programs_equal(result_project.program, project.program)


def assert_sample_equal(result_sample: sql.Row, sample: Sample) -> None:
    assert result_sample.sample_type == sample.sample_type


def assert_tissue_source_sites_equal(
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


def assert_data_categories_equal(
    result_data_category: sql.Row, data_category: DataCategory
) -> None:
    assert result_data_category.data_category == data_category.data_category
    assert result_data_category.file_count == data_category.file_count


def assert_summaries_equal(result_summary: sql.Row, summary: Summary) -> None:
    assert result_summary.file_count == summary.file_count
    assert result_summary.file_size == summary.file_size

    for result_data_catetory, data_category in more_itertools.zip_equal(
        result_summary.data_categories, summary.data_categories
    ):
        assert_data_categories_equal(result_data_catetory, data_category)


def assert_cases_equal(result_case: sql.Row, case: Case) -> None:
    assert result_case.case_id == case.case_id
    assert result_case.consent_type == case.consent_type
    assert result_case.days_to_consent == case.days_to_consent
    assert result_case.disease_type == case.disease_type
    assert result_case.index_date == case.index_date
    assert result_case.lost_to_followup == case.lost_to_followup
    assert result_case.primary_site == case.primary_site
    assert result_case.state == case.state
    assert result_case.submitter_id == case.submitter_id

    assert_demographics_equal(result_case.demographic, case.demographic)
    assert_projects_equal(result_case.project, case.project)
    assert_tissue_source_sites_equal(
        result_case.tissue_source_site, case.tissue_source_site
    )
    assert_summaries_equal(result_case.summary, case.summary)

    for result_diagnosis, diagnosis in more_itertools.zip_equal(
        result_case.diagnoses, case.diagnoses
    ):
        assert_diagnoses_equal(result_diagnosis, diagnosis)

    for result_exposure, exposure in more_itertools.zip_equal(
        result_case.exposures, case.exposures
    ):
        assert_exposures_equal(result_exposure, exposure)

    for result_sample, sample in more_itertools.zip_equal(
        result_case.samples, case.samples
    ):
        assert_sample_equal(result_sample, sample)


class TestCaseBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        case_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.case_schema = case_schema
        self.final_schema = final_schema

    def arrange_es_dataframe_util(
        self, cases: Iterable[Case] = (Case(),)
    ) -> mock.MagicMock():
        util = mock.MagicMock()

        util.get_dataframe.return_value = self.spark_session.createDataFrame(
            cases, schema=self.case_schema
        )

        return util

    def arrange_input_dataframes(
        self,
        metadata_case_ids: Iterable[str] = (),
        ascat_case_ids: Iterable[str] = (),
    ) -> Dict[str, sql.DataFrame]:
        def to_rows(case_ids: Iterable[str]) -> Tuple[sql.Row, ...]:
            return tuple(sql.Row(case_id=case_id) for case_id in case_ids)

        maf_metadata_df = self.spark_session.createDataFrame(
            to_rows(metadata_case_ids), schema=CASE_ID_SCHEMA
        )
        ascat_df = self.spark_session.createDataFrame(
            to_rows(ascat_case_ids), schema=CASE_ID_SCHEMA
        ).withColumn("available_variation_data", F.lit("cnv"))

        return {
            "maf_metadata_df": maf_metadata_df,
            "ascat_df": ascat_df,
        }

    def test__build_from_scratch__single_row(self) -> None:
        config = viz.CaseBuilder(
            is_cached=False,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            excluded_fields=(),
            repartition_size=2048
        )
        sql_context = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util()
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, sql_context, es_dataframe_util)

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build_from_scratch__data_translated(self) -> None:
        config = viz.CaseBuilder(
            is_cached=False,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            excluded_fields=(),
            repartition_size=2048
        )
        case = Case()
        sql_context = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, sql_context, es_dataframe_util)

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert_cases_equal(result_row, case)

    @pytest.mark.parametrize(
        ("maf_metadata_cases", "ascat_cases", "available_variation_data"),
        (
            ((), (), frozenset(())),
            (("case-0",), (), frozenset(("ssm",))),
            ((), ("case-0",), frozenset(("cnv",))),
            (("case-0",), ("case-0",), frozenset(("cnv", "ssm"))),
        ),
        ids=("neither", "only-in-metadata", "only-in-ascat", "metadata-and-ascat"),
    )
    def test__build_from_scratch__available_variation_data(
        self,
        maf_metadata_cases: Iterable[str],
        ascat_cases: Iterable[str],
        available_variation_data: FrozenSet[str],
    ) -> None:
        config = viz.CaseBuilder(
            is_cached=False,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            excluded_fields=(),
            repartition_size=2048
        )
        case = Case(case_id="case-0")
        sql_context = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        inputs = self.arrange_input_dataframes(maf_metadata_cases, ascat_cases)
        builder = builders.CaseBuilder(config, sql_context, es_dataframe_util)

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert (
            frozenset(result_row.available_variation_data or ())
            == available_variation_data
        )
