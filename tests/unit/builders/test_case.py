import dataclasses
from collections.abc import Iterable
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration
from tests.unit.data import schemas

CASE_ID_SCHEMA = "case_id: string"


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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.age_at_index == self.age_at_index
            assert row.age_is_obfuscated == self.age_is_obfuscated
            assert row.cause_of_death == self.cause_of_death
            assert row.cause_of_death_source == self.cause_of_death_source
            assert row.country_of_birth == self.country_of_birth
            assert (
                row.country_of_residence_at_enrollment
                == self.country_of_residence_at_enrollment
            )
            assert row.days_to_birth == self.days_to_birth
            assert row.days_to_death == self.days_to_death
            assert row.demographic_id == self.demographic_id
            assert row.education_level == self.education_level
            assert row.ethnicity == self.ethnicity
            assert row.gender == self.gender
            assert row.marital_status == self.marital_status
            assert row.occupation_duration_years == self.occupation_duration_years
            assert row.population_group == self.population_group
            assert row.race == self.race
            assert row.sex_at_birth == self.sex_at_birth
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.vital_status == self.vital_status
            assert row.year_of_birth == self.year_of_birth
            assert row.year_of_death == self.year_of_death

            return True

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

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.additional_pathology_findings == self.additional_pathology_findings
                assert row.anaplasia_present == self.anaplasia_present
                assert row.anaplasia_present_type == self.anaplasia_present_type
                assert row.bone_marrow_malignant_cells == self.bone_marrow_malignant_cells
                assert row.breslow_thickness == self.breslow_thickness
                assert row.breslow_thickness_category == self.breslow_thickness_category
                assert (
                    row.circumferential_resection_margin
                    == self.circumferential_resection_margin
                )
                assert row.columnar_mucosa_present == self.columnar_mucosa_present
                assert row.consistent_pathology_review == self.consistent_pathology_review
                assert row.created_datetime == self.created_datetime
                assert row.days_to_pathology_detail == self.days_to_pathology_detail
                assert row.dysplasia_degree == self.dysplasia_degree
                assert row.dysplasia_type == self.dysplasia_type
                assert row.epithelioid_cell_percent == self.epithelioid_cell_percent
                assert (
                    row.epithelioid_cell_percent_range == self.epithelioid_cell_percent_range
                )
                assert row.extracapsular_extension == self.extracapsular_extension
                assert (
                    row.extracapsular_extension_present == self.extracapsular_extension_present
                )
                assert row.extranodal_extension == self.extranodal_extension
                assert row.extraocular_nodule_size == self.extraocular_nodule_size
                assert row.extrascleral_extension == self.extrascleral_extension
                assert (
                    row.extrascleral_extension_present == self.extrascleral_extension_present
                )
                assert row.extrathyroid_extension == self.extrathyroid_extension
                assert row.greatest_tumor_dimension == self.greatest_tumor_dimension
                assert row.gross_tumor_weight == self.gross_tumor_weight
                assert row.histologic_progression_type == self.histologic_progression_type
                assert (
                    row.intratubular_germ_cell_neoplasia_present
                    == self.intratubular_germ_cell_neoplasia_present
                )
                assert (
                    row.largest_extrapelvic_peritoneal_focus
                    == self.largest_extrapelvic_peritoneal_focus
                )
                assert row.lymph_node_dissection_method == self.lymph_node_dissection_method
                assert row.lymph_node_dissection_site == self.lymph_node_dissection_site
                assert row.lymph_node_involved_site == self.lymph_node_involved_site
                assert row.lymph_node_involvement == self.lymph_node_involvement
                assert row.lymph_nodes_positive == self.lymph_nodes_positive
                assert row.lymph_nodes_removed == self.lymph_nodes_removed
                assert row.lymph_nodes_tested == self.lymph_nodes_tested
                assert row.lymphatic_invasion_present == self.lymphatic_invasion_present
                assert row.margin_status == self.margin_status
                assert row.measurement_type == self.measurement_type
                assert row.measurement_unit == self.measurement_unit
                assert row.metaplasia_present == self.metaplasia_present
                assert row.micrometastasis_present == self.micrometastasis_present
                assert (
                    row.morphologic_architectural_pattern
                    == self.morphologic_architectural_pattern
                )
                assert row.necrosis_percent == self.necrosis_percent
                assert row.necrosis_present == self.necrosis_present
                assert row.non_nodal_regional_disease == self.non_nodal_regional_disease
                assert row.non_nodal_tumor_deposits == self.non_nodal_tumor_deposits
                assert row.number_proliferating_cells == self.number_proliferating_cells
                assert row.pathology_detail_id == self.pathology_detail_id
                assert row.percent_tumor_invasion == self.percent_tumor_invasion
                assert row.percent_tumor_nuclei == self.percent_tumor_nuclei
                assert row.perineural_invasion_present == self.perineural_invasion_present
                assert (
                    row.peripancreatic_lymph_nodes_positive
                    == self.peripancreatic_lymph_nodes_positive
                )
                assert (
                    row.peripancreatic_lymph_nodes_tested
                    == self.peripancreatic_lymph_nodes_tested
                )
                assert row.prcc_type == self.prcc_type
                assert (
                    row.prostatic_chips_positive_count == self.prostatic_chips_positive_count
                )
                assert row.prostatic_chips_total_count == self.prostatic_chips_total_count
                assert row.prostatic_involvement_percent == self.prostatic_involvement_percent
                assert row.residual_tumor == self.residual_tumor
                assert row.residual_tumor_measurement == self.residual_tumor_measurement
                assert row.rhabdoid_percent == self.rhabdoid_percent
                assert row.rhabdoid_present == self.rhabdoid_present
                assert row.sarcomatoid_percent == self.sarcomatoid_percent
                assert row.sarcomatoid_present == self.sarcomatoid_present
                assert row.size_extraocular_nodule == self.size_extraocular_nodule
                assert row.spindle_cell_percent == self.spindle_cell_percent
                assert row.spindle_cell_percent_range == self.spindle_cell_percent_range
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.timepoint_category == self.timepoint_category
                assert row.transglottic_extension == self.transglottic_extension
                assert row.tumor_basal_diameter == self.tumor_basal_diameter
                assert row.tumor_burden == self.tumor_burden
                assert row.tumor_depth_descriptor == self.tumor_depth_descriptor
                assert row.tumor_depth_measurement == self.tumor_depth_measurement
                assert (
                    row.tumor_infiltrating_lymphocytes == self.tumor_infiltrating_lymphocytes
                )
                assert (
                    row.tumor_infiltrating_macrophages == self.tumor_infiltrating_macrophages
                )
                assert (
                    row.tumor_largest_dimension_diameter
                    == self.tumor_largest_dimension_diameter
                )
                assert row.tumor_length_measurement == self.tumor_length_measurement
                assert row.tumor_shape == self.tumor_shape
                assert row.tumor_thickness == self.tumor_thickness
                assert row.tumor_width_measurement == self.tumor_width_measurement
                assert row.updated_datetime == self.updated_datetime
                assert row.vascular_invasion_present == self.vascular_invasion_present
                assert row.vascular_invasion_type == self.vascular_invasion_type
                assert row.zone_of_origin_prostate == self.zone_of_origin_prostate
                assert tuple(row.tumor_level_prostate) == self.tumor_level_prostate

                return True

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

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.chemo_concurrent_to_radiation == self.chemo_concurrent_to_radiation
                assert row.clinical_trial_indicator == self.clinical_trial_indicator
                assert row.course_number == self.course_number
                assert row.created_datetime == self.created_datetime
                assert row.days_to_treatment_end == self.days_to_treatment_end
                assert row.days_to_treatment_start == self.days_to_treatment_start
                assert row.drug_category == self.drug_category
                assert row.embolic_agent == self.embolic_agent
                assert row.initial_disease_status == self.initial_disease_status
                assert row.lesions_treated_number == self.lesions_treated_number
                assert row.margin_distance == self.margin_distance
                assert row.margin_status == self.margin_status
                assert row.margins_involved_site == self.margins_involved_site
                assert row.number_of_cycles == self.number_of_cycles
                assert row.number_of_fractions == self.number_of_fractions
                assert row.prescribed_dose == self.prescribed_dose
                assert row.prescribed_dose_units == self.prescribed_dose_units
                assert row.pretreatment == self.pretreatment
                assert row.protocol_identifier == self.protocol_identifier
                assert row.radiosensitizing_agent == self.radiosensitizing_agent
                assert row.reason_treatment_ended == self.reason_treatment_ended
                assert row.reason_treatment_not_given == self.reason_treatment_not_given
                assert row.regimen_or_line_of_therapy == self.regimen_or_line_of_therapy
                assert row.residual_disease == self.residual_disease
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.therapeutic_agents == self.therapeutic_agents
                assert row.therapeutic_level_achieved == self.therapeutic_level_achieved
                assert row.therapeutic_levels_achieved == self.therapeutic_levels_achieved
                assert row.therapeutic_target_level == self.therapeutic_target_level
                assert row.timepoint_category == self.timepoint_category
                assert row.treatment_dose == self.treatment_dose
                assert row.treatment_dose_max == self.treatment_dose_max
                assert row.treatment_dose_units == self.treatment_dose_units
                assert row.treatment_duration == self.treatment_duration
                assert row.treatment_effect == self.treatment_effect
                assert row.treatment_effect_indicator == self.treatment_effect_indicator
                assert row.treatment_frequency == self.treatment_frequency
                assert row.treatment_id == self.treatment_id
                assert row.treatment_intent_type == self.treatment_intent_type
                assert row.treatment_or_therapy == self.treatment_or_therapy
                assert row.treatment_outcome == self.treatment_outcome
                assert row.treatment_outcome_duration == self.treatment_outcome_duration
                assert row.treatment_type == self.treatment_type
                assert row.updated_datetime == self.updated_datetime
                assert tuple(row.route_of_administration) == self.route_of_administration
                assert tuple(row.treatment_anatomic_sites) == self.treatment_anatomic_sites

                return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.adrenal_hormone == self.adrenal_hormone
            assert row.age_at_diagnosis == self.age_at_diagnosis
            assert row.ajcc_clinical_m == self.ajcc_clinical_m
            assert row.ajcc_clinical_n == self.ajcc_clinical_n
            assert row.ajcc_clinical_stage == self.ajcc_clinical_stage
            assert row.ajcc_clinical_t == self.ajcc_clinical_t
            assert row.ajcc_pathologic_m == self.ajcc_pathologic_m
            assert row.ajcc_pathologic_n == self.ajcc_pathologic_n
            assert row.ajcc_pathologic_stage == self.ajcc_pathologic_stage
            assert row.ajcc_pathologic_t == self.ajcc_pathologic_t
            assert row.ajcc_serum_tumor_markers == self.ajcc_serum_tumor_markers
            assert row.ajcc_staging_system_edition == self.ajcc_staging_system_edition
            assert row.ann_arbor_b_symptoms == self.ann_arbor_b_symptoms
            assert row.ann_arbor_b_symptoms_described == self.ann_arbor_b_symptoms_described
            assert row.ann_arbor_clinical_stage == self.ann_arbor_clinical_stage
            assert (
                row.ann_arbor_extranodal_involvement == self.ann_arbor_extranodal_involvement
            )
            assert row.ann_arbor_pathologic_stage == self.ann_arbor_pathologic_stage
            assert row.best_overall_response == self.best_overall_response
            assert (
                row.burkitt_lymphoma_clinical_variant == self.burkitt_lymphoma_clinical_variant
            )
            assert row.calgb_risk_group == self.calgb_risk_group
            assert row.cancer_detection_method == self.cancer_detection_method
            assert row.child_pugh_classification == self.child_pugh_classification
            assert row.clark_level == self.clark_level
            assert row.classification_of_tumor == self.classification_of_tumor
            assert row.cog_liver_stage == self.cog_liver_stage
            assert row.cog_neuroblastoma_risk_group == self.cog_neuroblastoma_risk_group
            assert row.cog_renal_stage == self.cog_renal_stage
            assert row.cog_rhabdomyosarcoma_risk_group == self.cog_rhabdomyosarcoma_risk_group
            assert row.contiguous_organ_invaded == self.contiguous_organ_invaded
            assert row.days_to_best_overall_response == self.days_to_best_overall_response
            assert row.days_to_diagnosis == self.days_to_diagnosis
            assert row.days_to_last_follow_up == self.days_to_last_follow_up
            assert (
                row.days_to_last_known_disease_status == self.days_to_last_known_disease_status
            )
            assert row.days_to_recurrence == self.days_to_recurrence
            assert row.diagnosis_id == self.diagnosis_id
            assert row.diagnosis_is_primary_disease == self.diagnosis_is_primary_disease
            assert row.double_expressor_lymphoma == self.double_expressor_lymphoma
            assert row.double_hit_lymphoma == self.double_hit_lymphoma
            assert row.eln_risk_classification == self.eln_risk_classification
            assert row.enneking_msts_grade == self.enneking_msts_grade
            assert row.enneking_msts_metastasis == self.enneking_msts_metastasis
            assert row.enneking_msts_stage == self.enneking_msts_stage
            assert row.enneking_msts_tumor_site == self.enneking_msts_tumor_site
            assert row.ensat_clinical_m == self.ensat_clinical_m
            assert row.ensat_pathologic_n == self.ensat_pathologic_n
            assert row.ensat_pathologic_stage == self.ensat_pathologic_stage
            assert row.ensat_pathologic_t == self.ensat_pathologic_t
            assert (
                row.esophageal_columnar_dysplasia_degree
                == self.esophageal_columnar_dysplasia_degree
            )
            assert (
                row.esophageal_columnar_metaplasia_present
                == self.esophageal_columnar_metaplasia_present
            )
            assert row.fab_morphology_code == self.fab_morphology_code
            assert row.figo_stage == self.figo_stage
            assert row.figo_staging_edition_year == self.figo_staging_edition_year
            assert row.first_symptom_longest_duration == self.first_symptom_longest_duration
            assert (
                row.first_symptom_prior_to_diagnosis == self.first_symptom_prior_to_diagnosis
            )
            assert (
                row.gastric_esophageal_junction_involvement
                == self.gastric_esophageal_junction_involvement
            )
            assert row.gleason_grade_group == self.gleason_grade_group
            assert row.gleason_grade_tertiary == self.gleason_grade_tertiary
            assert row.gleason_patterns_percent == self.gleason_patterns_percent
            assert row.gleason_score == self.gleason_score
            assert (
                row.goblet_cells_columnar_mucosa_present
                == self.goblet_cells_columnar_mucosa_present
            )
            assert row.icd_10_code == self.icd_10_code
            assert row.igcccg_stage == self.igcccg_stage
            assert row.inpc_grade == self.inpc_grade
            assert row.inpc_histologic_group == self.inpc_histologic_group
            assert row.inrg_stage == self.inrg_stage
            assert row.inss_stage == self.inss_stage
            assert row.international_prognostic_index == self.international_prognostic_index
            assert row.irs_group == self.irs_group
            assert row.irs_stage == self.irs_stage
            assert row.ishak_fibrosis_score == self.ishak_fibrosis_score
            assert row.iss_stage == self.iss_stage
            assert row.last_known_disease_status == self.last_known_disease_status
            assert row.laterality == self.laterality
            assert row.margin_distance == self.margin_distance
            assert row.margins_involved_site == self.margins_involved_site
            assert row.masaoka_stage == self.masaoka_stage
            assert row.max_tumor_bulk_site == self.max_tumor_bulk_site
            assert (
                row.medulloblastoma_molecular_classification
                == self.medulloblastoma_molecular_classification
            )
            assert row.melanoma_known_primary == self.melanoma_known_primary
            assert row.metastasis_at_diagnosis == self.metastasis_at_diagnosis
            assert row.method_of_diagnosis == self.method_of_diagnosis
            assert row.mitosis_karyorrhexis_index == self.mitosis_karyorrhexis_index
            assert row.morphology == self.morphology
            assert row.ovarian_specimen_status == self.ovarian_specimen_status
            assert row.ovarian_surface_involvement == self.ovarian_surface_involvement
            assert row.pediatric_kidney_staging == self.pediatric_kidney_staging
            assert (
                row.peritoneal_fluid_cytological_status
                == self.peritoneal_fluid_cytological_status
            )
            assert row.primary_diagnosis == self.primary_diagnosis
            assert row.primary_gleason_grade == self.primary_gleason_grade
            assert row.prior_malignancy == self.prior_malignancy
            assert row.prior_treatment == self.prior_treatment
            assert row.progression_or_recurrence == self.progression_or_recurrence
            assert row.residual_disease == self.residual_disease
            assert row.satellite_nodule_present == self.satellite_nodule_present
            assert row.secondary_gleason_grade == self.secondary_gleason_grade
            assert row.site_of_resection_or_biopsy == self.site_of_resection_or_biopsy
            assert row.sites_of_involvement_count == self.sites_of_involvement_count
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.supratentorial_localization == self.supratentorial_localization
            assert row.synchronous_malignancy == self.synchronous_malignancy
            assert row.tissue_or_organ_of_origin == self.tissue_or_organ_of_origin
            assert row.tumor_burden == self.tumor_burden
            assert (
                row.tumor_confined_to_organ_of_origin == self.tumor_confined_to_organ_of_origin
            )
            assert row.tumor_depth == self.tumor_depth
            assert row.tumor_focality == self.tumor_focality
            assert row.tumor_grade == self.tumor_grade
            assert row.tumor_grade_category == self.tumor_grade_category
            assert row.tumor_of_origin == self.tumor_of_origin
            assert row.tumor_regression_grade == self.tumor_regression_grade
            assert row.uicc_clinical_m == self.uicc_clinical_m
            assert row.uicc_clinical_n == self.uicc_clinical_n
            assert row.uicc_clinical_stage == self.uicc_clinical_stage
            assert row.uicc_clinical_t == self.uicc_clinical_t
            assert row.uicc_pathologic_m == self.uicc_pathologic_m
            assert row.uicc_pathologic_n == self.uicc_pathologic_n
            assert row.uicc_pathologic_stage == self.uicc_pathologic_stage
            assert row.uicc_pathologic_t == self.uicc_pathologic_t
            assert row.uicc_staging_system_edition == self.uicc_staging_system_edition
            assert row.ulceration_indicator == self.ulceration_indicator
            assert row.weiss_assessment_score == self.weiss_assessment_score
            assert row.who_cns_grade == self.who_cns_grade
            assert row.who_nte_grade == self.who_nte_grade
            assert row.wilms_tumor_histologic_subtype == self.wilms_tumor_histologic_subtype
            assert row.year_of_diagnosis == self.year_of_diagnosis
            assert tuple(row.sites_of_involvement) == self.sites_of_involvement
            assert tuple(row.weiss_assessment_findings) == self.weiss_assessment_findings
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.pathology_details or (), self.pathology_details or ())
            )
            assert all(
                e.assert_equals(r) for r, e in zip(row.treatments or (), self.treatments or ())
            )

            return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.age_at_last_exposure == self.age_at_last_exposure
            assert row.age_at_onset == self.age_at_onset
            assert row.alcohol_days_per_week == self.alcohol_days_per_week
            assert row.alcohol_drinks_per_day == self.alcohol_drinks_per_day
            assert row.alcohol_frequency == self.alcohol_frequency
            assert row.alcohol_history == self.alcohol_history
            assert row.alcohol_intensity == self.alcohol_intensity
            assert row.alcohol_type == self.alcohol_type
            assert row.asbestos_exposure_type == self.asbestos_exposure_type
            assert row.cigarettes_per_day == self.cigarettes_per_day
            assert (
                row.environmental_tobacco_smoke_exposure
                == self.environmental_tobacco_smoke_exposure
            )
            assert row.exposure_duration == self.exposure_duration
            assert row.exposure_duration_hrs_per_day == self.exposure_duration_hrs_per_day
            assert row.exposure_duration_years == self.exposure_duration_years
            assert row.exposure_id == self.exposure_id
            assert row.exposure_source == self.exposure_source
            assert row.exposure_type == self.exposure_type
            assert row.occupation_duration_years == self.occupation_duration_years
            assert row.pack_years_smoked == self.pack_years_smoked
            assert row.parent_with_radiation_exposure == self.parent_with_radiation_exposure
            assert row.secondhand_smoke_as_child == self.secondhand_smoke_as_child
            assert row.smoking_frequency == self.smoking_frequency
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert (
                row.time_between_waking_and_first_smoke
                == self.time_between_waking_and_first_smoke
            )
            assert row.tobacco_smoking_onset_year == self.tobacco_smoking_onset_year
            assert row.tobacco_smoking_quit_year == self.tobacco_smoking_quit_year
            assert row.tobacco_smoking_status == self.tobacco_smoking_status
            assert row.type_of_smoke_exposure == self.type_of_smoke_exposure
            assert row.type_of_tobacco_used == self.type_of_tobacco_used
            assert row.use_per_day == self.use_per_day
            assert tuple(row.chemical_exposure_type) == self.chemical_exposure_type
            assert tuple(row.occupation_type) == self.occupation_type

            return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.family_history_id == self.family_history_id
            assert row.relationship_age_at_diagnosis == self.relationship_age_at_diagnosis
            assert row.relationship_gender == self.relationship_gender
            assert row.relationship_primary_diagnosis == self.relationship_primary_diagnosis
            assert row.relationship_sex_at_birth == self.relationship_sex_at_birth
            assert row.relationship_type == self.relationship_type
            assert row.relative_deceased == self.relative_deceased
            assert row.relative_smoker == self.relative_smoker
            assert row.relative_with_cancer_history == self.relative_with_cancer_history
            assert (
                row.relatives_with_cancer_history_count
                == self.relatives_with_cancer_history_count
            )
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id

            return True

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

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.aa_change == self.aa_change
                assert row.aneuploidy == self.aneuploidy
                assert row.antigen == self.antigen
                assert row.biospecimen_type == self.biospecimen_type
                assert row.biospecimen_volume == self.biospecimen_volume
                assert row.blood_test_normal_range_lower == self.blood_test_normal_range_lower
                assert row.blood_test_normal_range_upper == self.blood_test_normal_range_upper
                assert row.cell_count == self.cell_count
                assert row.chromosomal_translocation == self.chromosomal_translocation
                assert row.chromosome == self.chromosome
                assert row.chromosome_arm == self.chromosome_arm
                assert row.clonality == self.clonality
                assert row.copy_number == self.copy_number
                assert row.cytoband == self.cytoband
                assert row.days_to_test == self.days_to_test
                assert row.exon == self.exon
                assert row.gene_symbol == self.gene_symbol
                assert row.histone_family == self.histone_family
                assert row.histone_variant == self.histone_variant
                assert row.hpv_strain == self.hpv_strain
                assert row.intron == self.intron
                assert row.laboratory_test == self.laboratory_test
                assert row.loci_abnormal_count == self.loci_abnormal_count
                assert row.loci_count == self.loci_count
                assert row.locus == self.locus
                assert row.mismatch_repair_mutation == self.mismatch_repair_mutation
                assert row.mitotic_count == self.mitotic_count
                assert row.mitotic_total_area == self.mitotic_total_area
                assert row.molecular_analysis_method == self.molecular_analysis_method
                assert row.molecular_consequence == self.molecular_consequence
                assert row.molecular_test_id == self.molecular_test_id
                assert row.mutation_codon == self.mutation_codon
                assert row.pathogenicity == self.pathogenicity
                assert row.ploidy == self.ploidy
                assert row.second_exon == self.second_exon
                assert row.second_gene_symbol == self.second_gene_symbol
                assert row.specialized_molecular_test == self.specialized_molecular_test
                assert row.staining_intensity_scale == self.staining_intensity_scale
                assert row.staining_intensity_value == self.staining_intensity_value
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.test_analyte_type == self.test_analyte_type
                assert row.test_result == self.test_result
                assert row.test_units == self.test_units
                assert row.test_value == self.test_value
                assert row.test_value_range == self.test_value_range
                assert row.timepoint_category == self.timepoint_category
                assert row.transcript == self.transcript
                assert row.variant_origin == self.variant_origin
                assert row.variant_type == self.variant_type
                assert row.zygosity == self.zygosity

                return True

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

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.aids_risk_factors == self.aids_risk_factors
                assert row.bmi == self.bmi
                assert row.body_surface_area == self.body_surface_area
                assert row.cd4_count == self.cd4_count
                assert row.cdc_hiv_risk_factors == self.cdc_hiv_risk_factors
                assert (
                    row.comorbidity_method_of_diagnosis == self.comorbidity_method_of_diagnosis
                )
                assert row.days_to_comorbidity == self.days_to_comorbidity
                assert row.days_to_risk_factor == self.days_to_risk_factor
                assert row.diabetes_treatment_type == self.diabetes_treatment_type
                assert row.dlco_ref_predictive_percent == self.dlco_ref_predictive_percent
                assert row.exercise_frequency_weekly == self.exercise_frequency_weekly
                assert row.eye_color == self.eye_color
                assert row.fertility_history == self.fertility_history
                assert row.fev1_fvc_post_bronch_percent == self.fev1_fvc_post_bronch_percent
                assert row.fev1_fvc_pre_bronch_percent == self.fev1_fvc_pre_bronch_percent
                assert row.fev1_ref_post_bronch_percent == self.fev1_ref_post_bronch_percent
                assert row.fev1_ref_pre_bronch_percent == self.fev1_ref_pre_bronch_percent
                assert row.haart_treatment_indicator == self.haart_treatment_indicator
                assert row.height == self.height
                assert (
                    row.hepatitis_sustained_virological_response
                    == self.hepatitis_sustained_virological_response
                )
                assert row.hiv_viral_load == self.hiv_viral_load
                assert row.hormonal_contraceptive_type == self.hormonal_contraceptive_type
                assert row.hormonal_contraceptive_use == self.hormonal_contraceptive_use
                assert (
                    row.hormonal_replacement_therapy_status
                    == self.hormonal_replacement_therapy_status
                )
                assert (
                    row.hormone_replacement_therapy_type
                    == self.hormone_replacement_therapy_type
                )
                assert row.hysterectomy_margins_involved == self.hysterectomy_margins_involved
                assert row.hysterectomy_type == self.hysterectomy_type
                assert (
                    row.immunosuppressive_treatment_type
                    == self.immunosuppressive_treatment_type
                )
                assert row.menopause_status == self.menopause_status
                assert (
                    row.myasthenia_gravis_classification
                    == self.myasthenia_gravis_classification
                )
                assert row.nadir_cd4_count == self.nadir_cd4_count
                assert (
                    row.nononcologic_therapeutic_agents == self.nononcologic_therapeutic_agents
                )
                assert row.number_of_pregnancies == self.number_of_pregnancies
                assert row.other_clinical_attribute_id == self.other_clinical_attribute_id
                assert row.oxygen_use_indicator == self.oxygen_use_indicator
                assert row.oxygen_use_type == self.oxygen_use_type
                assert row.pancreatitis_onset_year == self.pancreatitis_onset_year
                assert row.pregnancy_outcome == self.pregnancy_outcome
                assert row.pregnant_at_diagnosis == self.pregnant_at_diagnosis
                assert row.premature_at_birth == self.premature_at_birth
                assert row.reflux_treatment_type == self.reflux_treatment_type
                assert (
                    row.risk_factor_method_of_diagnosis == self.risk_factor_method_of_diagnosis
                )
                assert row.risk_factor_treatment == self.risk_factor_treatment
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.timepoint_category == self.timepoint_category
                assert row.treatment_frequency == self.treatment_frequency
                assert row.undescended_testis_corrected == self.undescended_testis_corrected
                assert (
                    row.undescended_testis_corrected_age
                    == self.undescended_testis_corrected_age
                )
                assert (
                    row.undescended_testis_corrected_age_range
                    == self.undescended_testis_corrected_age_range
                )
                assert (
                    row.undescended_testis_corrected_laterality
                    == self.undescended_testis_corrected_laterality
                )
                assert (
                    row.undescended_testis_corrected_method
                    == self.undescended_testis_corrected_method
                )
                assert row.undescended_testis_history == self.undescended_testis_history
                assert (
                    row.undescended_testis_history_laterality
                    == self.undescended_testis_history_laterality
                )
                assert row.weeks_gestation_at_birth == self.weeks_gestation_at_birth
                assert row.weight == self.weight
                assert tuple(row.comorbidities) == self.comorbidities
                assert tuple(row.risk_factors) == self.risk_factors
                assert (
                    tuple(row.viral_hepatitis_serology_tests)
                    == self.viral_hepatitis_serology_tests
                )

                return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.adverse_event == self.adverse_event
            assert row.adverse_event_grade == self.adverse_event_grade
            assert (
                row.barretts_esophagus_goblet_cells_present
                == self.barretts_esophagus_goblet_cells_present
            )
            assert row.cause_of_response == self.cause_of_response
            assert row.days_to_adverse_event == self.days_to_adverse_event
            assert row.days_to_first_event == self.days_to_first_event
            assert row.days_to_follow_up == self.days_to_follow_up
            assert row.days_to_imaging == self.days_to_imaging
            assert row.days_to_progression == self.days_to_progression
            assert row.days_to_progression_free == self.days_to_progression_free
            assert row.days_to_recurrence == self.days_to_recurrence
            assert row.discontiguous_lesion_count == self.discontiguous_lesion_count
            assert row.disease_response == self.disease_response
            assert row.ecog_performance_status == self.ecog_performance_status
            assert row.evidence_of_progression_type == self.evidence_of_progression_type
            assert row.evidence_of_recurrence_type == self.evidence_of_recurrence_type
            assert row.first_event == self.first_event
            assert row.follow_up_id == self.follow_up_id
            assert row.histologic_progression == self.histologic_progression
            assert row.history_of_tumor == self.history_of_tumor
            assert row.history_of_tumor_type == self.history_of_tumor_type
            assert (
                row.hormone_replacement_therapy_type == self.hormone_replacement_therapy_type
            )
            assert row.imaging_findings == self.imaging_findings
            assert row.imaging_result == self.imaging_result
            assert row.imaging_suv == self.imaging_suv
            assert row.imaging_suv_max == self.imaging_suv_max
            assert row.imaging_type == self.imaging_type
            assert row.karnofsky_performance_status == self.karnofsky_performance_status
            assert row.peritoneal_washing_results == self.peritoneal_washing_results
            assert row.procedures_performed == self.procedures_performed
            assert row.progression_or_recurrence == self.progression_or_recurrence
            assert (
                row.progression_or_recurrence_anatomic_site
                == self.progression_or_recurrence_anatomic_site
            )
            assert row.progression_or_recurrence_type == self.progression_or_recurrence_type
            assert row.recist_targeted_regions_number == self.recist_targeted_regions_number
            assert row.recist_targeted_regions_sum == self.recist_targeted_regions_sum
            assert row.scan_tracer_used == self.scan_tracer_used
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.timepoint_category == self.timepoint_category
            assert (
                row.treatment_emergent_adverse_event == self.treatment_emergent_adverse_event
            )
            assert row.year_of_follow_up == self.year_of_follow_up
            assert tuple(row.imaging_anatomic_site) == self.imaging_anatomic_site
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.molecular_tests or (), self.molecular_tests or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(
                    row.other_clinical_attributes or (),
                    self.other_clinical_attributes or (),
                )
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Project:
        @dataclasses.dataclass(frozen=True)
        class Program:
            dbgap_accession_number: str | None = "phs000178"
            name: str | None = "Baylor College of Medicine"
            program_id: str | None = "program-0"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.dbgap_accession_number == self.dbgap_accession_number
                assert row.name == self.name
                assert row.program_id == self.program_id

                return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.dbgap_accession_number == self.dbgap_accession_number
            assert row.intended_release_date == self.intended_release_date
            assert row.name == self.name
            assert row.project_id == self.project_id
            assert row.releasable == self.releasable
            assert row.released == self.released
            assert row.state == self.state
            assert tuple(row.disease_type) == self.disease_type
            assert tuple(row.primary_site) == self.primary_site
            assert (row.program is None and self.program is None) or (
                self.program and self.program.assert_equals(row.program)
            )

            return True

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.biospecimen_anatomic_site == self.biospecimen_anatomic_site
            assert row.biospecimen_laterality == self.biospecimen_laterality
            assert row.catalog_reference == self.catalog_reference
            assert row.current_weight == self.current_weight
            assert row.days_to_collection == self.days_to_collection
            assert row.days_to_sample_procurement == self.days_to_sample_procurement
            assert (
                row.diagnosis_pathologically_confirmed
                == self.diagnosis_pathologically_confirmed
            )
            assert row.distance_normal_to_tumor == self.distance_normal_to_tumor
            assert row.distributor_reference == self.distributor_reference
            assert row.freezing_method == self.freezing_method
            assert row.growth_rate == self.growth_rate
            assert row.initial_weight == self.initial_weight
            assert row.intermediate_dimension == self.intermediate_dimension
            assert row.longest_dimension == self.longest_dimension
            assert row.method_of_sample_procurement == self.method_of_sample_procurement
            assert row.passage_count == self.passage_count
            assert row.pathology_report_uuid == self.pathology_report_uuid
            assert row.preservation_method == self.preservation_method
            assert row.sample_id == self.sample_id
            assert row.sample_ordinal == self.sample_ordinal
            assert row.sample_type == self.sample_type
            assert row.shortest_dimension == self.shortest_dimension
            assert row.specimen_type == self.specimen_type
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert (
                row.time_between_clamping_and_freezing
                == self.time_between_clamping_and_freezing
            )
            assert (
                row.time_between_excision_and_freezing
                == self.time_between_excision_and_freezing
            )
            assert row.tissue_collection_type == self.tissue_collection_type
            assert row.tissue_type == self.tissue_type
            assert row.tumor_code_id == self.tumor_code_id
            assert row.tumor_descriptor == self.tumor_descriptor

            return True

    @dataclasses.dataclass(frozen=True)
    class TissueSourceSite:
        bcr_id: str | None = "IGC"
        code: str | None = "10"
        name: str | None = "Baylor College of Medicine"
        project: str | None = "Ovarian serous cystadenocarcinoma"
        tissue_source_site_id: str | None = "tissue-source-site-0"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.bcr_id == self.bcr_id
            assert row.code == self.code
            assert row.name == self.name
            assert row.project == self.project
            assert row.tissue_source_site_id == self.tissue_source_site_id

            return True

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

    def assert_equals(self, row: sql.Row) -> bool:
        assert row
        assert row.case_id == self.case_id
        assert row.consent_type == self.consent_type
        assert row.days_to_consent == self.days_to_consent
        assert row.days_to_lost_to_followup == self.days_to_lost_to_followup
        assert row.disease_type == self.disease_type
        assert row.index_date == self.index_date
        assert row.lost_to_followup == self.lost_to_followup
        assert row.primary_site == self.primary_site
        assert row.state == self.state
        assert row.submitter_id == self.submitter_id
        assert (row.demographic is None and self.demographic is None) or (
            self.demographic and self.demographic.assert_equals(row.demographic)
        )
        assert (row.project is None and self.project is None) or (
            self.project and self.project.assert_equals(row.project)
        )
        assert (row.tissue_source_site is None and self.tissue_source_site is None) or (
            self.tissue_source_site
            and self.tissue_source_site.assert_equals(row.tissue_source_site)
        )
        assert all(
            e.assert_equals(r) for r, e in zip(row.diagnoses or (), self.diagnoses or ())
        )
        assert all(
            e.assert_equals(r) for r, e in zip(row.exposures or (), self.exposures or ())
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.family_histories or (), self.family_histories or ())
        )
        assert all(
            e.assert_equals(r) for r, e in zip(row.follow_ups or (), self.follow_ups or ())
        )
        assert all(e.assert_equals(r) for r, e in zip(row.samples or (), self.samples or ()))

        return True


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.RAW.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.Case.FINAL.load()


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

    def arrange_config(self) -> configuration.CaseBuilder:
        backup = mock.MagicMock(mode=build.BackupMode.NEITHER, path="")

        return mock.MagicMock(
            spec=configuration.CaseBuilder,
            projects=(),
            repartition_size=1,
            include_as_arrays=(),
            is_cached=False,
            backup=backup,
        )

    def arrange_es_dataframe_util(
        self, cases: Iterable[Case] = (Case(),)
    ) -> es_utils.DataFrameUtil:
        util = mock.MagicMock(spec=es_utils.DataFrameUtil)

        util.read.return_value = self.spark_session.createDataFrame(
            cases,  # type: ignore
            schema=self.case_schema,
        )

        return util

    def arrange_input_dataframes(
        self,
        maf_metadata_case_ids: Iterable[str] = (),
        ascat_metadata_case_ids: Iterable[str] = (),
        segment_cnv_metadata_ids: Iterable[str] = (),
    ) -> dict[str, sql.DataFrame]:
        def to_rows(case_ids: Iterable[str]) -> tuple[sql.Row, ...]:
            return tuple(sql.Row(case_id=case_id) for case_id in case_ids)

        maf_metadata_df = self.spark_session.createDataFrame(
            to_rows(maf_metadata_case_ids), schema=CASE_ID_SCHEMA
        )
        ascat_metadata_df = self.spark_session.createDataFrame(
            to_rows(ascat_metadata_case_ids), schema=CASE_ID_SCHEMA
        )
        segment_cnv_metadata_df = self.spark_session.createDataFrame(
            to_rows(segment_cnv_metadata_ids), schema=CASE_ID_SCHEMA
        )
        return {
            "maf_metadata_df": maf_metadata_df,
            "ascat_metadata_df": ascat_metadata_df,
            "segment_cnv_metadata_df": segment_cnv_metadata_df,
        }

    def arrange_case_field_selector(self) -> es_utils.CaseFieldSelector:
        selector = mock.MagicMock(spec=es_utils.CaseFieldSelector)
        selector.select_for.return_value = ()

        return selector

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util()
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        config = self.arrange_config()
        case = Case()
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes()
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        case.assert_equals(result_row)

    @pytest.mark.parametrize(
        (
            "maf_metadata_cases",
            "ascat_metadata_cases",
            "segment_cnv_metadata_cases",
            "available_variation_data",
        ),
        (
            ((), (), (), frozenset(())),
            (("case-0",), (), (), frozenset(("ssm",))),
            ((), ("case-0",), (), frozenset(("cnv",))),
            ((), (), ("case-0",), frozenset(("segment_cnv",))),
            (
                ("case-0",),
                ("case-0",),
                ("case-0",),
                frozenset(("cnv", "ssm", "segment_cnv")),
            ),
        ),
        ids=(
            "neither",
            "only-in-metadata",
            "only-in-ascat",
            "only-in-segment-cnv",
            "metadata-and-ascat",
        ),
    )
    def test__build__available_variation_data(
        self,
        maf_metadata_cases: Iterable[str],
        ascat_metadata_cases: Iterable[str],
        segment_cnv_metadata_cases: Iterable[str],
        available_variation_data: frozenset[str],
    ) -> None:
        config = self.arrange_config()
        case = Case(case_id="case-0")
        spark_session = mock.MagicMock()
        es_dataframe_util = self.arrange_es_dataframe_util((case,))
        selector = self.arrange_case_field_selector()
        inputs = self.arrange_input_dataframes(
            maf_metadata_cases, ascat_metadata_cases, segment_cnv_metadata_cases
        )
        builder = builders.CaseBuilder(config, spark_session, es_dataframe_util, selector)

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert frozenset(result_row.available_variation_data or ()) == available_variation_data
