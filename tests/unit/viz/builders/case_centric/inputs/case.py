import dataclasses
from typing import Optional

from pyspark import sql


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Annotation:
        annotation_id: Optional[str] = "annotation-0"
        case_id: Optional[str] = "case-0"
        case_submitter_id: Optional[str] = None
        category: Optional[str] = "General"
        classification: Optional[str] = "Observation"
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        creator: Optional[str] = None
        entity_id: Optional[str] = "entity-0"
        entity_submitter_id: Optional[str] = "sub-entity-0"
        entity_type: Optional[str] = "aliquot"
        legacy_created_datetime: Optional[str] = None
        legacy_updated_datetime: Optional[str] = None
        notes: Optional[
            str
        ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
        state: Optional[str] = "released"
        status: Optional[str] = "Approved"
        submitter_id: Optional[str] = "sub-generic-0"
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.annotation_id == self.annotation_id
            assert row.case_id == self.case_id
            assert row.case_submitter_id == self.case_submitter_id
            assert row.category == self.category
            assert row.classification == self.classification
            assert row.created_datetime == self.created_datetime
            assert row.creator == self.creator
            assert row.entity_id == self.entity_id
            assert row.entity_submitter_id == self.entity_submitter_id
            assert row.entity_type == self.entity_type
            assert row.legacy_created_datetime == self.legacy_created_datetime
            assert row.legacy_updated_datetime == self.legacy_updated_datetime
            assert row.notes == self.notes
            assert row.state == self.state
            assert row.status == self.status
            assert row.submitter_id == self.submitter_id
            assert row.updated_datetime == self.updated_datetime

            return True

    @dataclasses.dataclass(frozen=True)
    class Demographic:
        age_at_index: Optional[int] = 60
        age_is_obfuscated: Optional[str] = None
        cause_of_death: Optional[str] = None
        cause_of_death_source: Optional[str] = None
        country_of_residence_at_enrollment: Optional[str] = None
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        days_to_birth: Optional[int] = -22052
        days_to_death: Optional[int] = 1324
        demographic_id: Optional[str] = "demographic-0"
        ethnicity: Optional[str] = "not hispanic or latino"
        gender: Optional[str] = "female"
        occupation_duration_years: Optional[int] = None
        premature_at_birth: Optional[str] = None
        race: Optional[str] = "white"
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
        vital_status: Optional[str] = "Alive"
        weeks_gestation_at_birth: Optional[float] = None
        year_of_birth: Optional[int] = 1951
        year_of_death: Optional[int] = 2009

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.age_at_index == self.age_at_index
            assert row.age_is_obfuscated == self.age_is_obfuscated
            assert row.cause_of_death == self.cause_of_death
            assert row.cause_of_death_source == self.cause_of_death_source
            assert (
                row.country_of_residence_at_enrollment
                == self.country_of_residence_at_enrollment
            )
            assert row.created_datetime == self.created_datetime
            assert row.days_to_birth == self.days_to_birth
            assert row.days_to_death == self.days_to_death
            assert row.demographic_id == self.demographic_id
            assert row.ethnicity == self.ethnicity
            assert row.gender == self.gender
            assert row.occupation_duration_years == self.occupation_duration_years
            assert row.premature_at_birth == self.premature_at_birth
            assert row.race == self.race
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.updated_datetime == self.updated_datetime
            assert row.vital_status == self.vital_status
            assert row.weeks_gestation_at_birth == self.weeks_gestation_at_birth
            assert row.year_of_birth == self.year_of_birth
            assert row.year_of_death == self.year_of_death

            return True

    @dataclasses.dataclass(frozen=True)
    class Diagnosis:
        @dataclasses.dataclass(frozen=True)
        class Annotation:
            annotation_id: Optional[str] = "annotation-0"
            case_id: Optional[str] = "case-0"
            case_submitter_id: Optional[str] = None
            category: Optional[str] = "General"
            classification: Optional[str] = "Observation"
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            creator: Optional[str] = None
            entity_id: Optional[str] = "entity-0"
            entity_submitter_id: Optional[str] = "sub-entity-0"
            entity_type: Optional[str] = "aliquot"
            legacy_created_datetime: Optional[str] = None
            legacy_updated_datetime: Optional[str] = None
            notes: Optional[
                str
            ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
            state: Optional[str] = "released"
            status: Optional[str] = "Approved"
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.annotation_id == self.annotation_id
                assert row.case_id == self.case_id
                assert row.case_submitter_id == self.case_submitter_id
                assert row.category == self.category
                assert row.classification == self.classification
                assert row.created_datetime == self.created_datetime
                assert row.creator == self.creator
                assert row.entity_id == self.entity_id
                assert row.entity_submitter_id == self.entity_submitter_id
                assert row.entity_type == self.entity_type
                assert row.legacy_created_datetime == self.legacy_created_datetime
                assert row.legacy_updated_datetime == self.legacy_updated_datetime
                assert row.notes == self.notes
                assert row.state == self.state
                assert row.status == self.status
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime

                return True

        @dataclasses.dataclass(frozen=True)
        class PathologyDetail:
            additional_pathology_findings: Optional[str] = None
            anaplasia_present: Optional[str] = None
            anaplasia_present_type: Optional[str] = None
            bone_marrow_malignant_cells: Optional[str] = None
            breslow_thickness: Optional[float] = None
            circumferential_resection_margin: Optional[float] = None
            columnar_mucosa_present: Optional[str] = None
            consistent_pathology_review: Optional[str] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
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
            necrosis_percent: Optional[float] = None
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
            residual_tumor: Optional[str] = None
            rhabdoid_percent: Optional[float] = None
            rhabdoid_present: Optional[str] = None
            sarcomatoid_percent: Optional[float] = None
            sarcomatoid_present: Optional[str] = None
            size_extraocular_nodule: Optional[float] = None
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            transglottic_extension: Optional[str] = None
            tumor_largest_dimension_diameter: Optional[float] = None
            tumor_thickness: Optional[float] = None
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
            vascular_invasion_present: Optional[str] = None
            vascular_invasion_type: Optional[str] = None

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert (
                    row.additional_pathology_findings
                    == self.additional_pathology_findings
                )
                assert row.anaplasia_present == self.anaplasia_present
                assert row.anaplasia_present_type == self.anaplasia_present_type
                assert (
                    row.bone_marrow_malignant_cells == self.bone_marrow_malignant_cells
                )
                assert row.breslow_thickness == self.breslow_thickness
                assert (
                    row.circumferential_resection_margin
                    == self.circumferential_resection_margin
                )
                assert row.columnar_mucosa_present == self.columnar_mucosa_present
                assert (
                    row.consistent_pathology_review == self.consistent_pathology_review
                )
                assert row.created_datetime == self.created_datetime
                assert row.dysplasia_degree == self.dysplasia_degree
                assert row.dysplasia_type == self.dysplasia_type
                assert row.greatest_tumor_dimension == self.greatest_tumor_dimension
                assert row.gross_tumor_weight == self.gross_tumor_weight
                assert (
                    row.largest_extrapelvic_peritoneal_focus
                    == self.largest_extrapelvic_peritoneal_focus
                )
                assert row.lymph_node_involved_site == self.lymph_node_involved_site
                assert row.lymph_node_involvement == self.lymph_node_involvement
                assert row.lymph_nodes_positive == self.lymph_nodes_positive
                assert row.lymph_nodes_tested == self.lymph_nodes_tested
                assert row.lymphatic_invasion_present == self.lymphatic_invasion_present
                assert row.margin_status == self.margin_status
                assert row.metaplasia_present == self.metaplasia_present
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
                assert (
                    row.perineural_invasion_present == self.perineural_invasion_present
                )
                assert (
                    row.peripancreatic_lymph_nodes_positive
                    == self.peripancreatic_lymph_nodes_positive
                )
                assert (
                    row.peripancreatic_lymph_nodes_tested
                    == self.peripancreatic_lymph_nodes_tested
                )
                assert (
                    row.prostatic_chips_positive_count
                    == self.prostatic_chips_positive_count
                )
                assert (
                    row.prostatic_chips_total_count == self.prostatic_chips_total_count
                )
                assert (
                    row.prostatic_involvement_percent
                    == self.prostatic_involvement_percent
                )
                assert row.residual_tumor == self.residual_tumor
                assert row.rhabdoid_percent == self.rhabdoid_percent
                assert row.rhabdoid_present == self.rhabdoid_present
                assert row.sarcomatoid_percent == self.sarcomatoid_percent
                assert row.sarcomatoid_present == self.sarcomatoid_present
                assert row.size_extraocular_nodule == self.size_extraocular_nodule
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.transglottic_extension == self.transglottic_extension
                assert (
                    row.tumor_largest_dimension_diameter
                    == self.tumor_largest_dimension_diameter
                )
                assert row.tumor_thickness == self.tumor_thickness
                assert row.updated_datetime == self.updated_datetime
                assert row.vascular_invasion_present == self.vascular_invasion_present
                assert row.vascular_invasion_type == self.vascular_invasion_type

                return True

        @dataclasses.dataclass(frozen=True)
        class Treatment:
            chemo_concurrent_to_radiation: Optional[str] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            days_to_treatment_end: Optional[int] = None
            days_to_treatment_start: Optional[int] = None
            initial_disease_status: Optional[str] = None
            number_of_cycles: Optional[int] = None
            reason_treatment_ended: Optional[str] = None
            regimen_or_line_of_therapy: Optional[str] = None
            route_of_administration: Optional[str] = None
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            therapeutic_agents: Optional[str] = None
            treatment_anatomic_site: Optional[str] = None
            treatment_arm: Optional[str] = None
            treatment_dose: Optional[int] = None
            treatment_dose_units: Optional[str] = None
            treatment_effect: Optional[str] = None
            treatment_effect_indicator: Optional[str] = None
            treatment_frequency: Optional[str] = None
            treatment_id: Optional[str] = "treatment-0"
            treatment_intent_type: Optional[str] = None
            treatment_or_therapy: Optional[str] = "yes"
            treatment_outcome: Optional[str] = None
            treatment_type: Optional[str] = "Radiation Therapy, NOS"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert (
                    row.chemo_concurrent_to_radiation
                    == self.chemo_concurrent_to_radiation
                )
                assert row.created_datetime == self.created_datetime
                assert row.days_to_treatment_end == self.days_to_treatment_end
                assert row.days_to_treatment_start == self.days_to_treatment_start
                assert row.initial_disease_status == self.initial_disease_status
                assert row.number_of_cycles == self.number_of_cycles
                assert row.reason_treatment_ended == self.reason_treatment_ended
                assert row.regimen_or_line_of_therapy == self.regimen_or_line_of_therapy
                assert row.route_of_administration == self.route_of_administration
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.therapeutic_agents == self.therapeutic_agents
                assert row.treatment_anatomic_site == self.treatment_anatomic_site
                assert row.treatment_arm == self.treatment_arm
                assert row.treatment_dose == self.treatment_dose
                assert row.treatment_dose_units == self.treatment_dose_units
                assert row.treatment_effect == self.treatment_effect
                assert row.treatment_effect_indicator == self.treatment_effect_indicator
                assert row.treatment_frequency == self.treatment_frequency
                assert row.treatment_id == self.treatment_id
                assert row.treatment_intent_type == self.treatment_intent_type
                assert row.treatment_or_therapy == self.treatment_or_therapy
                assert row.treatment_outcome == self.treatment_outcome
                assert row.treatment_type == self.treatment_type
                assert row.updated_datetime == self.updated_datetime

                return True

        adrenal_hormone: Optional[str] = None
        age_at_diagnosis: Optional[int] = 22052
        ajcc_clinical_m: Optional[str] = None
        ajcc_clinical_n: Optional[str] = None
        ajcc_clinical_stage: Optional[str] = None
        ajcc_clinical_t: Optional[str] = None
        ajcc_pathologic_m: Optional[str] = "M0"
        ajcc_pathologic_n: Optional[str] = "N0"
        ajcc_pathologic_stage: Optional[str] = "Stage II"
        ajcc_pathologic_t: Optional[str] = "T2"
        ajcc_staging_system_edition: Optional[str] = "6th"
        ann_arbor_b_symptoms: Optional[str] = None
        ann_arbor_b_symptoms_described: Optional[str] = None
        ann_arbor_clinical_stage: Optional[str] = None
        ann_arbor_extranodal_involvement: Optional[str] = None
        ann_arbor_pathologic_stage: Optional[str] = None
        annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
        best_overall_response: Optional[str] = None
        burkitt_lymphoma_clinical_variant: Optional[str] = None
        child_pugh_classification: Optional[str] = None
        classification_of_tumor: Optional[str] = "not reported"
        cog_liver_stage: Optional[str] = None
        cog_neuroblastoma_risk_group: Optional[str] = None
        cog_renal_stage: Optional[str] = None
        cog_rhabdomyosarcoma_risk_group: Optional[str] = None
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        days_to_best_overall_response: Optional[int] = None
        days_to_diagnosis: Optional[int] = 0
        days_to_last_follow_up: Optional[float] = 795.0
        days_to_last_known_disease_status: Optional[float] = None
        days_to_recurrence: Optional[float] = None
        diagnosis_id: Optional[str] = "diagnosis-0"
        eln_risk_classification: Optional[str] = None
        enneking_msts_grade: Optional[str] = None
        enneking_msts_metastasis: Optional[str] = None
        enneking_msts_stage: Optional[str] = None
        enneking_msts_tumor_site: Optional[str] = None
        esophageal_columnar_dysplasia_degree: Optional[str] = None
        esophageal_columnar_metaplasia_present: Optional[str] = None
        figo_stage: Optional[str] = None
        figo_staging_edition_year: Optional[str] = None
        first_symptom_prior_to_diagnosis: Optional[str] = None
        gastric_esophageal_junction_involvement: Optional[str] = None
        gleason_grade_group: Optional[str] = None
        gleason_grade_tertiary: Optional[str] = None
        gleason_patterns_percent: Optional[int] = None
        goblet_cells_columnar_mucosa_present: Optional[str] = None
        icd_10_code: Optional[str] = "C50.9"
        igcccg_stage: Optional[str] = None
        inpc_grade: Optional[str] = None
        inpc_histologic_group: Optional[str] = None
        inrg_stage: Optional[str] = None
        inss_stage: Optional[str] = None
        international_prognostic_index: Optional[str] = None
        irs_group: Optional[str] = None
        irs_stage: Optional[str] = None
        ishak_fibrosis_score: Optional[str] = None
        iss_stage: Optional[str] = None
        last_known_disease_status: Optional[str] = "not reported"
        laterality: Optional[str] = None
        margin_distance: Optional[float] = None
        margins_involved_site: Optional[str] = None
        masaoka_stage: Optional[str] = None
        medulloblastoma_molecular_classification: Optional[str] = None
        metastasis_at_diagnosis: Optional[str] = None
        metastasis_at_diagnosis_site: Optional[str] = None
        method_of_diagnosis: Optional[str] = None
        micropapillary_features: Optional[str] = None
        mitosis_karyorrhexis_index: Optional[str] = None
        mitotic_count: Optional[int] = None
        morphology: Optional[str] = "8500/3"
        ovarian_specimen_status: Optional[str] = None
        ovarian_surface_involvement: Optional[str] = None
        papillary_renal_cell_type: Optional[str] = None
        pathology_details: Optional[tuple[PathologyDetail, ...]] = (PathologyDetail(),)
        peritoneal_fluid_cytological_status: Optional[str] = None
        pregnant_at_diagnosis: Optional[str] = None
        primary_diagnosis: Optional[str] = "Infiltrating duct carcinoma, NOS"
        primary_disease: Optional[str] = None
        primary_gleason_grade: Optional[str] = None
        prior_malignancy: Optional[str] = "no"
        prior_treatment: Optional[str] = "No"
        progression_or_recurrence: Optional[str] = "not reported"
        residual_disease: Optional[str] = None
        satellite_nodule_present: Optional[str] = None
        secondary_gleason_grade: Optional[str] = None
        site_of_resection_or_biopsy: Optional[str] = "Breast, NOS"
        sites_of_involvement: Optional[str] = None
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        supratentorial_localization: Optional[str] = None
        synchronous_malignancy: Optional[str] = "No"
        tissue_or_organ_of_origin: Optional[str] = "Breast, NOS"
        treatments: Optional[tuple[Treatment, ...]] = (Treatment(),)
        tumor_confined_to_organ_of_origin: Optional[str] = None
        tumor_depth: Optional[float] = None
        tumor_focality: Optional[str] = None
        tumor_grade: Optional[str] = "not reported"
        tumor_regression_grade: Optional[str] = None
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
        weiss_assessment_score: Optional[str] = None
        who_cns_grade: Optional[str] = None
        who_nte_grade: Optional[str] = None
        wilms_tumor_histologic_subtype: Optional[str] = None
        year_of_diagnosis: Optional[int] = 2011

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
            assert row.ajcc_staging_system_edition == self.ajcc_staging_system_edition
            assert row.ann_arbor_b_symptoms == self.ann_arbor_b_symptoms
            assert (
                row.ann_arbor_b_symptoms_described
                == self.ann_arbor_b_symptoms_described
            )
            assert row.ann_arbor_clinical_stage == self.ann_arbor_clinical_stage
            assert (
                row.ann_arbor_extranodal_involvement
                == self.ann_arbor_extranodal_involvement
            )
            assert row.ann_arbor_pathologic_stage == self.ann_arbor_pathologic_stage
            assert row.best_overall_response == self.best_overall_response
            assert (
                row.burkitt_lymphoma_clinical_variant
                == self.burkitt_lymphoma_clinical_variant
            )
            assert row.child_pugh_classification == self.child_pugh_classification
            assert row.classification_of_tumor == self.classification_of_tumor
            assert row.cog_liver_stage == self.cog_liver_stage
            assert row.cog_neuroblastoma_risk_group == self.cog_neuroblastoma_risk_group
            assert row.cog_renal_stage == self.cog_renal_stage
            assert (
                row.cog_rhabdomyosarcoma_risk_group
                == self.cog_rhabdomyosarcoma_risk_group
            )
            assert row.created_datetime == self.created_datetime
            assert (
                row.days_to_best_overall_response == self.days_to_best_overall_response
            )
            assert row.days_to_diagnosis == self.days_to_diagnosis
            assert row.days_to_last_follow_up == self.days_to_last_follow_up
            assert (
                row.days_to_last_known_disease_status
                == self.days_to_last_known_disease_status
            )
            assert row.days_to_recurrence == self.days_to_recurrence
            assert row.diagnosis_id == self.diagnosis_id
            assert row.eln_risk_classification == self.eln_risk_classification
            assert row.enneking_msts_grade == self.enneking_msts_grade
            assert row.enneking_msts_metastasis == self.enneking_msts_metastasis
            assert row.enneking_msts_stage == self.enneking_msts_stage
            assert row.enneking_msts_tumor_site == self.enneking_msts_tumor_site
            assert (
                row.esophageal_columnar_dysplasia_degree
                == self.esophageal_columnar_dysplasia_degree
            )
            assert (
                row.esophageal_columnar_metaplasia_present
                == self.esophageal_columnar_metaplasia_present
            )
            assert row.figo_stage == self.figo_stage
            assert row.figo_staging_edition_year == self.figo_staging_edition_year
            assert (
                row.first_symptom_prior_to_diagnosis
                == self.first_symptom_prior_to_diagnosis
            )
            assert (
                row.gastric_esophageal_junction_involvement
                == self.gastric_esophageal_junction_involvement
            )
            assert row.gleason_grade_group == self.gleason_grade_group
            assert row.gleason_grade_tertiary == self.gleason_grade_tertiary
            assert row.gleason_patterns_percent == self.gleason_patterns_percent
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
            assert (
                row.international_prognostic_index
                == self.international_prognostic_index
            )
            assert row.irs_group == self.irs_group
            assert row.irs_stage == self.irs_stage
            assert row.ishak_fibrosis_score == self.ishak_fibrosis_score
            assert row.iss_stage == self.iss_stage
            assert row.last_known_disease_status == self.last_known_disease_status
            assert row.laterality == self.laterality
            assert row.margin_distance == self.margin_distance
            assert row.margins_involved_site == self.margins_involved_site
            assert row.masaoka_stage == self.masaoka_stage
            assert (
                row.medulloblastoma_molecular_classification
                == self.medulloblastoma_molecular_classification
            )
            assert row.metastasis_at_diagnosis == self.metastasis_at_diagnosis
            assert row.metastasis_at_diagnosis_site == self.metastasis_at_diagnosis_site
            assert row.method_of_diagnosis == self.method_of_diagnosis
            assert row.micropapillary_features == self.micropapillary_features
            assert row.mitosis_karyorrhexis_index == self.mitosis_karyorrhexis_index
            assert row.mitotic_count == self.mitotic_count
            assert row.morphology == self.morphology
            assert row.ovarian_specimen_status == self.ovarian_specimen_status
            assert row.ovarian_surface_involvement == self.ovarian_surface_involvement
            assert row.papillary_renal_cell_type == self.papillary_renal_cell_type
            assert (
                row.peritoneal_fluid_cytological_status
                == self.peritoneal_fluid_cytological_status
            )
            assert row.pregnant_at_diagnosis == self.pregnant_at_diagnosis
            assert row.primary_diagnosis == self.primary_diagnosis
            assert row.primary_disease == self.primary_disease
            assert row.primary_gleason_grade == self.primary_gleason_grade
            assert row.prior_malignancy == self.prior_malignancy
            assert row.prior_treatment == self.prior_treatment
            assert row.progression_or_recurrence == self.progression_or_recurrence
            assert row.residual_disease == self.residual_disease
            assert row.satellite_nodule_present == self.satellite_nodule_present
            assert row.secondary_gleason_grade == self.secondary_gleason_grade
            assert row.site_of_resection_or_biopsy == self.site_of_resection_or_biopsy
            assert row.sites_of_involvement == self.sites_of_involvement
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.supratentorial_localization == self.supratentorial_localization
            assert row.synchronous_malignancy == self.synchronous_malignancy
            assert row.tissue_or_organ_of_origin == self.tissue_or_organ_of_origin
            assert (
                row.tumor_confined_to_organ_of_origin
                == self.tumor_confined_to_organ_of_origin
            )
            assert row.tumor_depth == self.tumor_depth
            assert row.tumor_focality == self.tumor_focality
            assert row.tumor_grade == self.tumor_grade
            assert row.tumor_regression_grade == self.tumor_regression_grade
            assert row.updated_datetime == self.updated_datetime
            assert row.weiss_assessment_score == self.weiss_assessment_score
            assert row.who_cns_grade == self.who_cns_grade
            assert row.who_nte_grade == self.who_nte_grade
            assert (
                row.wilms_tumor_histologic_subtype
                == self.wilms_tumor_histologic_subtype
            )
            assert row.year_of_diagnosis == self.year_of_diagnosis
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.annotations or (), self.annotations or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(
                    row.pathology_details or (), self.pathology_details or ()
                )
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.treatments or (), self.treatments or ())
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Exposure:
        age_at_onset: Optional[int] = None
        alcohol_days_per_week: Optional[float] = None
        alcohol_drinks_per_day: Optional[float] = None
        alcohol_history: Optional[str] = "Not Reported"
        alcohol_intensity: Optional[str] = None
        alcohol_type: Optional[str] = None
        asbestos_exposure: Optional[str] = None
        cigarettes_per_day: Optional[float] = None
        coal_dust_exposure: Optional[str] = None
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        environmental_tobacco_smoke_exposure: Optional[str] = None
        exposure_duration: Optional[str] = None
        exposure_duration_years: Optional[int] = None
        exposure_id: Optional[str] = "exposure-0"
        exposure_type: Optional[str] = None
        marijuana_use_per_week: Optional[float] = None
        pack_years_smoked: Optional[float] = None
        parent_with_radiation_exposure: Optional[str] = None
        radon_exposure: Optional[str] = None
        respirable_crystalline_silica_exposure: Optional[str] = None
        secondhand_smoke_as_child: Optional[str] = None
        smokeless_tobacco_quit_age: Optional[int] = None
        smoking_frequency: Optional[str] = None
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        time_between_waking_and_first_smoke: Optional[str] = None
        tobacco_smoking_onset_year: Optional[int] = None
        tobacco_smoking_quit_year: Optional[int] = None
        tobacco_smoking_status: Optional[str] = None
        tobacco_use_per_day: Optional[float] = None
        type_of_smoke_exposure: Optional[str] = None
        type_of_tobacco_used: Optional[str] = None
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
        years_smoked: Optional[float] = None

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.age_at_onset == self.age_at_onset
            assert row.alcohol_days_per_week == self.alcohol_days_per_week
            assert row.alcohol_drinks_per_day == self.alcohol_drinks_per_day
            assert row.alcohol_history == self.alcohol_history
            assert row.alcohol_intensity == self.alcohol_intensity
            assert row.alcohol_type == self.alcohol_type
            assert row.asbestos_exposure == self.asbestos_exposure
            assert row.cigarettes_per_day == self.cigarettes_per_day
            assert row.coal_dust_exposure == self.coal_dust_exposure
            assert row.created_datetime == self.created_datetime
            assert (
                row.environmental_tobacco_smoke_exposure
                == self.environmental_tobacco_smoke_exposure
            )
            assert row.exposure_duration == self.exposure_duration
            assert row.exposure_duration_years == self.exposure_duration_years
            assert row.exposure_id == self.exposure_id
            assert row.exposure_type == self.exposure_type
            assert row.marijuana_use_per_week == self.marijuana_use_per_week
            assert row.pack_years_smoked == self.pack_years_smoked
            assert (
                row.parent_with_radiation_exposure
                == self.parent_with_radiation_exposure
            )
            assert row.radon_exposure == self.radon_exposure
            assert (
                row.respirable_crystalline_silica_exposure
                == self.respirable_crystalline_silica_exposure
            )
            assert row.secondhand_smoke_as_child == self.secondhand_smoke_as_child
            assert row.smokeless_tobacco_quit_age == self.smokeless_tobacco_quit_age
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
            assert row.tobacco_use_per_day == self.tobacco_use_per_day
            assert row.type_of_smoke_exposure == self.type_of_smoke_exposure
            assert row.type_of_tobacco_used == self.type_of_tobacco_used
            assert row.updated_datetime == self.updated_datetime
            assert row.years_smoked == self.years_smoked

            return True

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        family_history_id: Optional[str] = None
        relationship_age_at_diagnosis: Optional[float] = None
        relationship_gender: Optional[str] = None
        relationship_primary_diagnosis: Optional[str] = None
        relationship_type: Optional[str] = None
        relative_with_cancer_history: Optional[str] = None
        relatives_with_cancer_history_count: Optional[int] = None
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.created_datetime == self.created_datetime
            assert row.family_history_id == self.family_history_id
            assert (
                row.relationship_age_at_diagnosis == self.relationship_age_at_diagnosis
            )
            assert row.relationship_gender == self.relationship_gender
            assert (
                row.relationship_primary_diagnosis
                == self.relationship_primary_diagnosis
            )
            assert row.relationship_type == self.relationship_type
            assert row.relative_with_cancer_history == self.relative_with_cancer_history
            assert (
                row.relatives_with_cancer_history_count
                == self.relatives_with_cancer_history_count
            )
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.updated_datetime == self.updated_datetime

            return True

    @dataclasses.dataclass(frozen=True)
    class File:
        @dataclasses.dataclass(frozen=True)
        class Analysi:
            @dataclasses.dataclass(frozen=True)
            class InputFile:
                access: Optional[str] = "open"
                average_base_quality: Optional[float] = 30.0
                average_insert_size: Optional[int] = 205
                average_read_length: Optional[int] = 100
                channel: Optional[str] = "Green"
                chip_id: Optional[str] = None
                chip_position: Optional[str] = None
                contamination: Optional[float] = 0.002546897560409226
                contamination_error: Optional[float] = 0.00043678932811308974
                created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                data_category: Optional[str] = "Biospecimen"
                data_format: Optional[str] = "BCR Biotab"
                data_type: Optional[str] = "Biospecimen Supplement"
                error_type: Optional[str] = "file_size"
                experimental_strategy: Optional[str] = "WXS"
                file_id: Optional[str] = "file-0"
                file_name: Optional[
                    str
                ] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
                file_size: Optional[int] = 353327
                imaging_date: Optional[str] = None
                magnification: Optional[float] = None
                md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
                mean_coverage: Optional[float] = 96.018325
                msi_score: Optional[float] = 0.010962821735
                msi_status: Optional[str] = "MSS"
                pairs_on_diff_chr: Optional[int] = 950137
                plate_name: Optional[str] = None
                plate_well: Optional[str] = None
                platform: Optional[str] = "Illumina Human Methylation 450"
                proc_internal: Optional[str] = None
                proportion_base_mismatch: Optional[float] = 0.006583998
                proportion_coverage_10x: Optional[float] = 0.901681
                proportion_coverage_30x: Optional[float] = 0.785325
                proportion_reads_duplicated: Optional[float] = 0.07279441150328658
                proportion_reads_mapped: Optional[float] = 0.9995056930268698
                proportion_targets_no_coverage: Optional[float] = 0.015333
                read_pair_number: Optional[str] = None
                revision: Optional[float] = None
                stain_type: Optional[str] = None
                state: Optional[str] = "released"
                state_comment: Optional[str] = None
                submitter_id: Optional[str] = "sub-generic-0"
                total_reads: Optional[int] = 154707508
                tumor_ploidy: Optional[float] = None
                tumor_purity: Optional[float] = None
                updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.access == self.access
                    assert row.average_base_quality == self.average_base_quality
                    assert row.average_insert_size == self.average_insert_size
                    assert row.average_read_length == self.average_read_length
                    assert row.channel == self.channel
                    assert row.chip_id == self.chip_id
                    assert row.chip_position == self.chip_position
                    assert row.contamination == self.contamination
                    assert row.contamination_error == self.contamination_error
                    assert row.created_datetime == self.created_datetime
                    assert row.data_category == self.data_category
                    assert row.data_format == self.data_format
                    assert row.data_type == self.data_type
                    assert row.error_type == self.error_type
                    assert row.experimental_strategy == self.experimental_strategy
                    assert row.file_id == self.file_id
                    assert row.file_name == self.file_name
                    assert row.file_size == self.file_size
                    assert row.imaging_date == self.imaging_date
                    assert row.magnification == self.magnification
                    assert row.md5sum == self.md5sum
                    assert row.mean_coverage == self.mean_coverage
                    assert row.msi_score == self.msi_score
                    assert row.msi_status == self.msi_status
                    assert row.pairs_on_diff_chr == self.pairs_on_diff_chr
                    assert row.plate_name == self.plate_name
                    assert row.plate_well == self.plate_well
                    assert row.platform == self.platform
                    assert row.proc_internal == self.proc_internal
                    assert row.proportion_base_mismatch == self.proportion_base_mismatch
                    assert row.proportion_coverage_10x == self.proportion_coverage_10x
                    assert row.proportion_coverage_30x == self.proportion_coverage_30x
                    assert (
                        row.proportion_reads_duplicated
                        == self.proportion_reads_duplicated
                    )
                    assert row.proportion_reads_mapped == self.proportion_reads_mapped
                    assert (
                        row.proportion_targets_no_coverage
                        == self.proportion_targets_no_coverage
                    )
                    assert row.read_pair_number == self.read_pair_number
                    assert row.revision == self.revision
                    assert row.stain_type == self.stain_type
                    assert row.state == self.state
                    assert row.state_comment == self.state_comment
                    assert row.submitter_id == self.submitter_id
                    assert row.total_reads == self.total_reads
                    assert row.tumor_ploidy == self.tumor_ploidy
                    assert row.tumor_purity == self.tumor_purity
                    assert row.updated_datetime == self.updated_datetime

                    return True

            @dataclasses.dataclass(frozen=True)
            class Metadatum:
                @dataclasses.dataclass(frozen=True)
                class ReadGroup:
                    @dataclasses.dataclass(frozen=True)
                    class ReadGroupQc:
                        adapter_content: Optional[str] = "PASS"
                        basic_statistics: Optional[str] = "PASS"
                        created_datetime: Optional[
                            str
                        ] = "2018-05-21T16:07:40.645885-05:00"
                        encoding: Optional[str] = "Sanger / Illumina 1.9"
                        fastq_name: Optional[str] = "122988_s.fq"
                        kmer_content: Optional[str] = "FAIL"
                        overrepresented_sequences: Optional[str] = "FAIL"
                        per_base_n_content: Optional[str] = "PASS"
                        per_base_sequence_content: Optional[str] = "FAIL"
                        per_base_sequence_quality: Optional[str] = "FAIL"
                        per_sequence_gc_content: Optional[str] = "FAIL"
                        per_sequence_quality_score: Optional[str] = "PASS"
                        per_tile_sequence_quality: Optional[str] = "PASS"
                        percent_gc_content: Optional[int] = 46
                        read_group_qc_id: Optional[str] = "read-group-qc-0"
                        sequence_duplication_levels: Optional[str] = "FAIL"
                        sequence_length_distribution: Optional[str] = "WARN"
                        state: Optional[str] = "released"
                        submitter_id: Optional[str] = "sub-generic-0"
                        total_sequences: Optional[int] = 4145948
                        updated_datetime: Optional[
                            str
                        ] = "2018-11-01T15:06:10.843096-05:00"
                        workflow_end_datetime: Optional[str] = None
                        workflow_link: Optional[
                            str
                        ] = "https://github.com/NCI-GDC/somatic-maf-cwl"
                        workflow_start_datetime: Optional[str] = None
                        workflow_type: Optional[
                            str
                        ] = "MuTect2 Variant Aggregation and Masking"
                        workflow_version: Optional[str] = "v1"

                        def assert_equals(self, row: sql.Row) -> bool:
                            assert row
                            assert row.adapter_content == self.adapter_content
                            assert row.basic_statistics == self.basic_statistics
                            assert row.created_datetime == self.created_datetime
                            assert row.encoding == self.encoding
                            assert row.fastq_name == self.fastq_name
                            assert row.kmer_content == self.kmer_content
                            assert (
                                row.overrepresented_sequences
                                == self.overrepresented_sequences
                            )
                            assert row.per_base_n_content == self.per_base_n_content
                            assert (
                                row.per_base_sequence_content
                                == self.per_base_sequence_content
                            )
                            assert (
                                row.per_base_sequence_quality
                                == self.per_base_sequence_quality
                            )
                            assert (
                                row.per_sequence_gc_content
                                == self.per_sequence_gc_content
                            )
                            assert (
                                row.per_sequence_quality_score
                                == self.per_sequence_quality_score
                            )
                            assert (
                                row.per_tile_sequence_quality
                                == self.per_tile_sequence_quality
                            )
                            assert row.percent_gc_content == self.percent_gc_content
                            assert row.read_group_qc_id == self.read_group_qc_id
                            assert (
                                row.sequence_duplication_levels
                                == self.sequence_duplication_levels
                            )
                            assert (
                                row.sequence_length_distribution
                                == self.sequence_length_distribution
                            )
                            assert row.state == self.state
                            assert row.submitter_id == self.submitter_id
                            assert row.total_sequences == self.total_sequences
                            assert row.updated_datetime == self.updated_datetime
                            assert (
                                row.workflow_end_datetime == self.workflow_end_datetime
                            )
                            assert row.workflow_link == self.workflow_link
                            assert (
                                row.workflow_start_datetime
                                == self.workflow_start_datetime
                            )
                            assert row.workflow_type == self.workflow_type
                            assert row.workflow_version == self.workflow_version

                            return True

                    adapter_name: Optional[str] = "Ad2.19+Ad1.16"
                    adapter_sequence: Optional[str] = None
                    base_caller_name: Optional[str] = None
                    base_caller_version: Optional[str] = None
                    chipseq_antibody: Optional[str] = None
                    chipseq_target: Optional[str] = None
                    created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                    days_to_sequencing: Optional[int] = None
                    experiment_name: Optional[str] = "TCGA-BH-A202-01A-11R-A14L-13"
                    flow_cell_barcode: Optional[str] = "HKF77BBXX"
                    fragment_maximum_length: Optional[int] = None
                    fragment_mean_length: Optional[float] = None
                    fragment_minimum_length: Optional[int] = None
                    fragment_standard_deviation_length: Optional[float] = None
                    fragmentation_enzyme: Optional[str] = None
                    includes_spike_ins: Optional[str] = None
                    instrument_model: Optional[str] = "Illumina HiSeq 4000"
                    is_paired_end: Optional[str] = "True"
                    lane_number: Optional[int] = 8
                    library_name: Optional[str] = "MX0440"
                    library_preparation_kit_catalog_number: Optional[str] = None
                    library_preparation_kit_name: Optional[str] = None
                    library_preparation_kit_vendor: Optional[str] = None
                    library_preparation_kit_version: Optional[str] = None
                    library_selection: Optional[str] = "miRNA Size Fractionation"
                    library_strand: Optional[str] = None
                    library_strategy: Optional[str] = "miRNA-Seq"
                    multiplex_barcode: Optional[str] = "TGGTCACA+TTGATGGA"
                    number_expect_cells: Optional[int] = None
                    platform: Optional[str] = "Illumina Human Methylation 450"
                    read_group_id: Optional[str] = "read-group-id"
                    read_group_name: Optional[str] = "122988"
                    read_group_qcs: Optional[tuple[ReadGroupQc, ...]] = (ReadGroupQc(),)
                    read_length: Optional[int] = 15
                    rin: Optional[float] = None
                    sequencing_center: Optional[str] = "BCGSC"
                    sequencing_date: Optional[str] = "2011-09-22T19"
                    single_cell_library: Optional[str] = None
                    size_selection_range: Optional[str] = None
                    spike_ins_concentration: Optional[str] = None
                    spike_ins_fasta: Optional[str] = None
                    state: Optional[str] = "released"
                    submitter_id: Optional[str] = "sub-generic-0"
                    target_capture_kit: Optional[str] = "Not Applicable"
                    target_capture_kit_catalog_number: Optional[str] = "NA"
                    target_capture_kit_name: Optional[
                        str
                    ] = "hg18 nimblegen exome version 2"
                    target_capture_kit_target_region: Optional[
                        str
                    ] = "ftp://genome.wustl.edu/pub/custom_capture/hg18_nimblegen_exome_version_2/hg18_nimblegen_exome_version_2.bed"
                    target_capture_kit_vendor: Optional[str] = "Nimblegen"
                    target_capture_kit_version: Optional[str] = None
                    to_trim_adapter_sequence: Optional[str] = "True"
                    updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                    def assert_equals(self, row: sql.Row) -> bool:
                        assert row
                        assert row.adapter_name == self.adapter_name
                        assert row.adapter_sequence == self.adapter_sequence
                        assert row.base_caller_name == self.base_caller_name
                        assert row.base_caller_version == self.base_caller_version
                        assert row.chipseq_antibody == self.chipseq_antibody
                        assert row.chipseq_target == self.chipseq_target
                        assert row.created_datetime == self.created_datetime
                        assert row.days_to_sequencing == self.days_to_sequencing
                        assert row.experiment_name == self.experiment_name
                        assert row.flow_cell_barcode == self.flow_cell_barcode
                        assert (
                            row.fragment_maximum_length == self.fragment_maximum_length
                        )
                        assert row.fragment_mean_length == self.fragment_mean_length
                        assert (
                            row.fragment_minimum_length == self.fragment_minimum_length
                        )
                        assert (
                            row.fragment_standard_deviation_length
                            == self.fragment_standard_deviation_length
                        )
                        assert row.fragmentation_enzyme == self.fragmentation_enzyme
                        assert row.includes_spike_ins == self.includes_spike_ins
                        assert row.instrument_model == self.instrument_model
                        assert row.is_paired_end == self.is_paired_end
                        assert row.lane_number == self.lane_number
                        assert row.library_name == self.library_name
                        assert (
                            row.library_preparation_kit_catalog_number
                            == self.library_preparation_kit_catalog_number
                        )
                        assert (
                            row.library_preparation_kit_name
                            == self.library_preparation_kit_name
                        )
                        assert (
                            row.library_preparation_kit_vendor
                            == self.library_preparation_kit_vendor
                        )
                        assert (
                            row.library_preparation_kit_version
                            == self.library_preparation_kit_version
                        )
                        assert row.library_selection == self.library_selection
                        assert row.library_strand == self.library_strand
                        assert row.library_strategy == self.library_strategy
                        assert row.multiplex_barcode == self.multiplex_barcode
                        assert row.number_expect_cells == self.number_expect_cells
                        assert row.platform == self.platform
                        assert row.read_group_id == self.read_group_id
                        assert row.read_group_name == self.read_group_name
                        assert row.read_length == self.read_length
                        assert row.rin == self.rin
                        assert row.sequencing_center == self.sequencing_center
                        assert row.sequencing_date == self.sequencing_date
                        assert row.single_cell_library == self.single_cell_library
                        assert row.size_selection_range == self.size_selection_range
                        assert (
                            row.spike_ins_concentration == self.spike_ins_concentration
                        )
                        assert row.spike_ins_fasta == self.spike_ins_fasta
                        assert row.state == self.state
                        assert row.submitter_id == self.submitter_id
                        assert row.target_capture_kit == self.target_capture_kit
                        assert (
                            row.target_capture_kit_catalog_number
                            == self.target_capture_kit_catalog_number
                        )
                        assert (
                            row.target_capture_kit_name == self.target_capture_kit_name
                        )
                        assert (
                            row.target_capture_kit_target_region
                            == self.target_capture_kit_target_region
                        )
                        assert (
                            row.target_capture_kit_vendor
                            == self.target_capture_kit_vendor
                        )
                        assert (
                            row.target_capture_kit_version
                            == self.target_capture_kit_version
                        )
                        assert (
                            row.to_trim_adapter_sequence
                            == self.to_trim_adapter_sequence
                        )
                        assert row.updated_datetime == self.updated_datetime
                        assert all(
                            e.assert_equals(r)
                            for r, e in zip(
                                row.read_group_qcs or (), self.read_group_qcs or ()
                            )
                        )

                        return True

                read_groups: Optional[tuple[ReadGroup, ...]] = (ReadGroup(),)

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert all(
                        e.assert_equals(r)
                        for r, e in zip(row.read_groups or (), self.read_groups or ())
                    )

                    return True

            analysis_id: Optional[str] = "analysis-0"
            analysis_type: Optional[str] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            input_files: Optional[tuple[InputFile, ...]] = (InputFile(),)
            metadata: Optional[Metadatum] = Metadatum()
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
            workflow_end_datetime: Optional[str] = None
            workflow_link: Optional[str] = "https://github.com/NCI-GDC/somatic-maf-cwl"
            workflow_start_datetime: Optional[str] = None
            workflow_type: Optional[str] = "MuTect2 Variant Aggregation and Masking"
            workflow_version: Optional[str] = "v1"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.analysis_id == self.analysis_id
                assert row.analysis_type == self.analysis_type
                assert row.created_datetime == self.created_datetime
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime
                assert row.workflow_end_datetime == self.workflow_end_datetime
                assert row.workflow_link == self.workflow_link
                assert row.workflow_start_datetime == self.workflow_start_datetime
                assert row.workflow_type == self.workflow_type
                assert row.workflow_version == self.workflow_version
                assert (row.metadata is None and self.metadata is None) or (
                    self.metadata and self.metadata.assert_equals(row.metadata)
                )
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.input_files or (), self.input_files or ())
                )

                return True

        @dataclasses.dataclass(frozen=True)
        class Archive:
            archive_id: Optional[str] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            data_category: Optional[str] = "Biospecimen"
            data_format: Optional[str] = "BCR Biotab"
            data_type: Optional[str] = "Biospecimen Supplement"
            error_type: Optional[str] = "file_size"
            file_name: Optional[
                str
            ] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: Optional[int] = 353327
            md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
            revision: Optional[float] = None
            state: Optional[str] = "released"
            state_comment: Optional[str] = None
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.archive_id == self.archive_id
                assert row.created_datetime == self.created_datetime
                assert row.data_category == self.data_category
                assert row.data_format == self.data_format
                assert row.data_type == self.data_type
                assert row.error_type == self.error_type
                assert row.file_name == self.file_name
                assert row.file_size == self.file_size
                assert row.md5sum == self.md5sum
                assert row.revision == self.revision
                assert row.state == self.state
                assert row.state_comment == self.state_comment
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime

                return True

        @dataclasses.dataclass(frozen=True)
        class Center:
            center_id: Optional[str] = "center-0"
            center_type: Optional[str] = "CGCC"
            code: Optional[str] = "20"
            name: Optional[str] = "MD Anderson - RPPA Core Facility (Proteomics)"
            namespace: Optional[str] = "mdanderson.org"
            short_name: Optional[str] = "MDA"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.center_id == self.center_id
                assert row.center_type == self.center_type
                assert row.code == self.code
                assert row.name == self.name
                assert row.namespace == self.namespace
                assert row.short_name == self.short_name

                return True

        @dataclasses.dataclass(frozen=True)
        class DownstreamAnalysis:
            @dataclasses.dataclass(frozen=True)
            class OutputFile:
                access: Optional[str] = "open"
                average_base_quality: Optional[float] = 30.0
                average_insert_size: Optional[int] = 205
                average_read_length: Optional[int] = 100
                channel: Optional[str] = "Green"
                chip_id: Optional[str] = None
                chip_position: Optional[str] = None
                contamination: Optional[float] = 0.002546897560409226
                contamination_error: Optional[float] = 0.00043678932811308974
                created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                data_category: Optional[str] = "Biospecimen"
                data_format: Optional[str] = "BCR Biotab"
                data_type: Optional[str] = "Biospecimen Supplement"
                error_type: Optional[str] = "file_size"
                experimental_strategy: Optional[str] = "WXS"
                file_id: Optional[str] = "file-0"
                file_name: Optional[
                    str
                ] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
                file_size: Optional[int] = 353327
                imaging_date: Optional[str] = None
                magnification: Optional[float] = None
                md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
                mean_coverage: Optional[float] = 96.018325
                msi_score: Optional[float] = 0.010962821735
                msi_status: Optional[str] = "MSS"
                pairs_on_diff_chr: Optional[int] = 950137
                plate_name: Optional[str] = None
                plate_well: Optional[str] = None
                platform: Optional[str] = "Illumina Human Methylation 450"
                proc_internal: Optional[str] = None
                proportion_base_mismatch: Optional[float] = 0.006583998
                proportion_coverage_10x: Optional[float] = 0.901681
                proportion_coverage_30x: Optional[float] = 0.785325
                proportion_reads_duplicated: Optional[float] = 0.07279441150328658
                proportion_reads_mapped: Optional[float] = 0.9995056930268698
                proportion_targets_no_coverage: Optional[float] = 0.015333
                read_pair_number: Optional[str] = None
                revision: Optional[float] = None
                stain_type: Optional[str] = None
                state: Optional[str] = "released"
                state_comment: Optional[str] = None
                submitter_id: Optional[str] = "sub-generic-0"
                total_reads: Optional[int] = 154707508
                tumor_ploidy: Optional[float] = None
                tumor_purity: Optional[float] = None
                updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.access == self.access
                    assert row.average_base_quality == self.average_base_quality
                    assert row.average_insert_size == self.average_insert_size
                    assert row.average_read_length == self.average_read_length
                    assert row.channel == self.channel
                    assert row.chip_id == self.chip_id
                    assert row.chip_position == self.chip_position
                    assert row.contamination == self.contamination
                    assert row.contamination_error == self.contamination_error
                    assert row.created_datetime == self.created_datetime
                    assert row.data_category == self.data_category
                    assert row.data_format == self.data_format
                    assert row.data_type == self.data_type
                    assert row.error_type == self.error_type
                    assert row.experimental_strategy == self.experimental_strategy
                    assert row.file_id == self.file_id
                    assert row.file_name == self.file_name
                    assert row.file_size == self.file_size
                    assert row.imaging_date == self.imaging_date
                    assert row.magnification == self.magnification
                    assert row.md5sum == self.md5sum
                    assert row.mean_coverage == self.mean_coverage
                    assert row.msi_score == self.msi_score
                    assert row.msi_status == self.msi_status
                    assert row.pairs_on_diff_chr == self.pairs_on_diff_chr
                    assert row.plate_name == self.plate_name
                    assert row.plate_well == self.plate_well
                    assert row.platform == self.platform
                    assert row.proc_internal == self.proc_internal
                    assert row.proportion_base_mismatch == self.proportion_base_mismatch
                    assert row.proportion_coverage_10x == self.proportion_coverage_10x
                    assert row.proportion_coverage_30x == self.proportion_coverage_30x
                    assert (
                        row.proportion_reads_duplicated
                        == self.proportion_reads_duplicated
                    )
                    assert row.proportion_reads_mapped == self.proportion_reads_mapped
                    assert (
                        row.proportion_targets_no_coverage
                        == self.proportion_targets_no_coverage
                    )
                    assert row.read_pair_number == self.read_pair_number
                    assert row.revision == self.revision
                    assert row.stain_type == self.stain_type
                    assert row.state == self.state
                    assert row.state_comment == self.state_comment
                    assert row.submitter_id == self.submitter_id
                    assert row.total_reads == self.total_reads
                    assert row.tumor_ploidy == self.tumor_ploidy
                    assert row.tumor_purity == self.tumor_purity
                    assert row.updated_datetime == self.updated_datetime

                    return True

            analysis_id: Optional[str] = "analysis-0"
            analysis_type: Optional[str] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            output_files: Optional[tuple[OutputFile, ...]] = (OutputFile(),)
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
            workflow_end_datetime: Optional[str] = None
            workflow_link: Optional[str] = "https://github.com/NCI-GDC/somatic-maf-cwl"
            workflow_start_datetime: Optional[str] = None
            workflow_type: Optional[str] = "MuTect2 Variant Aggregation and Masking"
            workflow_version: Optional[str] = "v1"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.analysis_id == self.analysis_id
                assert row.analysis_type == self.analysis_type
                assert row.created_datetime == self.created_datetime
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime
                assert row.workflow_end_datetime == self.workflow_end_datetime
                assert row.workflow_link == self.workflow_link
                assert row.workflow_start_datetime == self.workflow_start_datetime
                assert row.workflow_type == self.workflow_type
                assert row.workflow_version == self.workflow_version
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.output_files or (), self.output_files or ())
                )

                return True

        @dataclasses.dataclass(frozen=True)
        class IndexFile:
            access: Optional[str] = "open"
            average_base_quality: Optional[float] = 30.0
            average_insert_size: Optional[int] = 205
            average_read_length: Optional[int] = 100
            channel: Optional[str] = "Green"
            chip_id: Optional[str] = None
            chip_position: Optional[str] = None
            contamination: Optional[float] = 0.002546897560409226
            contamination_error: Optional[float] = 0.00043678932811308974
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            data_category: Optional[str] = "Biospecimen"
            data_format: Optional[str] = "BCR Biotab"
            data_type: Optional[str] = "Biospecimen Supplement"
            error_type: Optional[str] = "file_size"
            experimental_strategy: Optional[str] = "WXS"
            file_id: Optional[str] = "file-0"
            file_name: Optional[
                str
            ] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: Optional[int] = 353327
            imaging_date: Optional[str] = None
            magnification: Optional[float] = None
            md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
            mean_coverage: Optional[float] = 96.018325
            msi_score: Optional[float] = 0.010962821735
            msi_status: Optional[str] = "MSS"
            pairs_on_diff_chr: Optional[int] = 950137
            plate_name: Optional[str] = None
            plate_well: Optional[str] = None
            platform: Optional[str] = "Illumina Human Methylation 450"
            proc_internal: Optional[str] = None
            proportion_base_mismatch: Optional[float] = 0.006583998
            proportion_coverage_10x: Optional[float] = 0.901681
            proportion_coverage_30x: Optional[float] = 0.785325
            proportion_reads_duplicated: Optional[float] = 0.07279441150328658
            proportion_reads_mapped: Optional[float] = 0.9995056930268698
            proportion_targets_no_coverage: Optional[float] = 0.015333
            read_pair_number: Optional[str] = None
            revision: Optional[float] = None
            stain_type: Optional[str] = None
            state: Optional[str] = "released"
            state_comment: Optional[str] = None
            submitter_id: Optional[str] = "sub-generic-0"
            total_reads: Optional[int] = 154707508
            tumor_ploidy: Optional[float] = None
            tumor_purity: Optional[float] = None
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.access == self.access
                assert row.average_base_quality == self.average_base_quality
                assert row.average_insert_size == self.average_insert_size
                assert row.average_read_length == self.average_read_length
                assert row.channel == self.channel
                assert row.chip_id == self.chip_id
                assert row.chip_position == self.chip_position
                assert row.contamination == self.contamination
                assert row.contamination_error == self.contamination_error
                assert row.created_datetime == self.created_datetime
                assert row.data_category == self.data_category
                assert row.data_format == self.data_format
                assert row.data_type == self.data_type
                assert row.error_type == self.error_type
                assert row.experimental_strategy == self.experimental_strategy
                assert row.file_id == self.file_id
                assert row.file_name == self.file_name
                assert row.file_size == self.file_size
                assert row.imaging_date == self.imaging_date
                assert row.magnification == self.magnification
                assert row.md5sum == self.md5sum
                assert row.mean_coverage == self.mean_coverage
                assert row.msi_score == self.msi_score
                assert row.msi_status == self.msi_status
                assert row.pairs_on_diff_chr == self.pairs_on_diff_chr
                assert row.plate_name == self.plate_name
                assert row.plate_well == self.plate_well
                assert row.platform == self.platform
                assert row.proc_internal == self.proc_internal
                assert row.proportion_base_mismatch == self.proportion_base_mismatch
                assert row.proportion_coverage_10x == self.proportion_coverage_10x
                assert row.proportion_coverage_30x == self.proportion_coverage_30x
                assert (
                    row.proportion_reads_duplicated == self.proportion_reads_duplicated
                )
                assert row.proportion_reads_mapped == self.proportion_reads_mapped
                assert (
                    row.proportion_targets_no_coverage
                    == self.proportion_targets_no_coverage
                )
                assert row.read_pair_number == self.read_pair_number
                assert row.revision == self.revision
                assert row.stain_type == self.stain_type
                assert row.state == self.state
                assert row.state_comment == self.state_comment
                assert row.submitter_id == self.submitter_id
                assert row.total_reads == self.total_reads
                assert row.tumor_ploidy == self.tumor_ploidy
                assert row.tumor_purity == self.tumor_purity
                assert row.updated_datetime == self.updated_datetime

                return True

        @dataclasses.dataclass(frozen=True)
        class MetadataFile:
            access: Optional[str] = "open"
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            data_category: Optional[str] = "Biospecimen"
            data_format: Optional[str] = "BCR Biotab"
            data_type: Optional[str] = "Biospecimen Supplement"
            error_type: Optional[str] = "file_size"
            file_id: Optional[str] = "file-0"
            file_name: Optional[
                str
            ] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: Optional[int] = 353327
            md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
            state: Optional[str] = "released"
            state_comment: Optional[str] = None
            submitter_id: Optional[str] = "sub-generic-0"
            type: Optional[str] = "biospecimen_supplement"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.access == self.access
                assert row.created_datetime == self.created_datetime
                assert row.data_category == self.data_category
                assert row.data_format == self.data_format
                assert row.data_type == self.data_type
                assert row.error_type == self.error_type
                assert row.file_id == self.file_id
                assert row.file_name == self.file_name
                assert row.file_size == self.file_size
                assert row.md5sum == self.md5sum
                assert row.state == self.state
                assert row.state_comment == self.state_comment
                assert row.submitter_id == self.submitter_id
                assert row.type == self.type
                assert row.updated_datetime == self.updated_datetime

                return True

        access: Optional[str] = "open"
        acl: Optional[tuple[str, ...]] = ("open",)
        analysis: Optional[Analysi] = Analysi()
        archive: Optional[Archive] = Archive()
        average_base_quality: Optional[float] = 30.0
        average_insert_size: Optional[int] = 205
        average_read_length: Optional[int] = 100
        center: Optional[Center] = Center()
        channel: Optional[str] = "Green"
        chip_id: Optional[str] = None
        chip_position: Optional[str] = None
        contamination: Optional[float] = 0.002546897560409226
        contamination_error: Optional[float] = 0.00043678932811308974
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        data_category: Optional[str] = "Biospecimen"
        data_format: Optional[str] = "BCR Biotab"
        data_type: Optional[str] = "Biospecimen Supplement"
        downstream_analyses: Optional[tuple[DownstreamAnalysis, ...]] = (
            DownstreamAnalysis(),
        )
        error_type: Optional[str] = "file_size"
        experimental_strategy: Optional[str] = "WXS"
        file_id: Optional[str] = "file-0"
        file_name: Optional[str] = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
        file_size: Optional[int] = 353327
        imaging_date: Optional[str] = None
        index_files: Optional[tuple[IndexFile, ...]] = (IndexFile(),)
        magnification: Optional[float] = None
        md5sum: Optional[str] = "f694515fd191ab8d3c0b073e6f2fc7cb"
        mean_coverage: Optional[float] = 96.018325
        metadata_files: Optional[tuple[MetadataFile, ...]] = (MetadataFile(),)
        msi_score: Optional[float] = 0.010962821735
        msi_status: Optional[str] = "MSS"
        pairs_on_diff_chr: Optional[int] = 950137
        plate_name: Optional[str] = None
        plate_well: Optional[str] = None
        platform: Optional[str] = "Illumina Human Methylation 450"
        proc_internal: Optional[str] = None
        proportion_base_mismatch: Optional[float] = 0.006583998
        proportion_coverage_10x: Optional[float] = 0.901681
        proportion_coverage_30x: Optional[float] = 0.785325
        proportion_reads_duplicated: Optional[float] = 0.07279441150328658
        proportion_reads_mapped: Optional[float] = 0.9995056930268698
        proportion_targets_no_coverage: Optional[float] = 0.015333
        read_pair_number: Optional[str] = None
        revision: Optional[float] = None
        stain_type: Optional[str] = None
        state: Optional[str] = "released"
        state_comment: Optional[str] = None
        submitter_id: Optional[str] = "sub-generic-0"
        tags: Optional[str] = None
        total_reads: Optional[int] = 154707508
        tumor_ploidy: Optional[float] = None
        tumor_purity: Optional[float] = None
        type: Optional[str] = "biospecimen_supplement"
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.access == self.access
            assert row.average_base_quality == self.average_base_quality
            assert row.average_insert_size == self.average_insert_size
            assert row.average_read_length == self.average_read_length
            assert row.channel == self.channel
            assert row.chip_id == self.chip_id
            assert row.chip_position == self.chip_position
            assert row.contamination == self.contamination
            assert row.contamination_error == self.contamination_error
            assert row.created_datetime == self.created_datetime
            assert row.data_category == self.data_category
            assert row.data_format == self.data_format
            assert row.data_type == self.data_type
            assert row.error_type == self.error_type
            assert row.experimental_strategy == self.experimental_strategy
            assert row.file_id == self.file_id
            assert row.file_name == self.file_name
            assert row.file_size == self.file_size
            assert row.imaging_date == self.imaging_date
            assert row.magnification == self.magnification
            assert row.md5sum == self.md5sum
            assert row.mean_coverage == self.mean_coverage
            assert row.msi_score == self.msi_score
            assert row.msi_status == self.msi_status
            assert row.pairs_on_diff_chr == self.pairs_on_diff_chr
            assert row.plate_name == self.plate_name
            assert row.plate_well == self.plate_well
            assert row.platform == self.platform
            assert row.proc_internal == self.proc_internal
            assert row.proportion_base_mismatch == self.proportion_base_mismatch
            assert row.proportion_coverage_10x == self.proportion_coverage_10x
            assert row.proportion_coverage_30x == self.proportion_coverage_30x
            assert row.proportion_reads_duplicated == self.proportion_reads_duplicated
            assert row.proportion_reads_mapped == self.proportion_reads_mapped
            assert (
                row.proportion_targets_no_coverage
                == self.proportion_targets_no_coverage
            )
            assert row.read_pair_number == self.read_pair_number
            assert row.revision == self.revision
            assert row.stain_type == self.stain_type
            assert row.state == self.state
            assert row.state_comment == self.state_comment
            assert row.submitter_id == self.submitter_id
            assert row.tags == self.tags
            assert row.total_reads == self.total_reads
            assert row.tumor_ploidy == self.tumor_ploidy
            assert row.tumor_purity == self.tumor_purity
            assert row.type == self.type
            assert row.updated_datetime == self.updated_datetime
            assert tuple(row.acl) == self.acl
            assert (row.analysis is None and self.analysis is None) or (
                self.analysis and self.analysis.assert_equals(row.analysis)
            )
            assert (row.archive is None and self.archive is None) or (
                self.archive and self.archive.assert_equals(row.archive)
            )
            assert (row.center is None and self.center is None) or (
                self.center and self.center.assert_equals(row.center)
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(
                    row.downstream_analyses or (), self.downstream_analyses or ()
                )
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.index_files or (), self.index_files or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.metadata_files or (), self.metadata_files or ())
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class FollowUp:
        @dataclasses.dataclass(frozen=True)
        class MolecularTest:
            aa_change: Optional[str] = None
            antigen: Optional[str] = None
            biospecimen_type: Optional[str] = None
            biospecimen_volume: Optional[float] = None
            blood_test_normal_range_lower: Optional[float] = None
            blood_test_normal_range_upper: Optional[float] = None
            cell_count: Optional[int] = None
            chromosome: Optional[str] = None
            clonality: Optional[str] = None
            copy_number: Optional[float] = None
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            cytoband: Optional[str] = None
            days_to_test: Optional[int] = None
            exon: Optional[str] = None
            gene_symbol: Optional[str] = None
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
            molecular_analysis_method: Optional[str] = None
            molecular_consequence: Optional[str] = None
            molecular_test_id: Optional[str] = None
            pathogenicity: Optional[str] = None
            ploidy: Optional[str] = None
            second_exon: Optional[str] = None
            second_gene_symbol: Optional[str] = None
            specialized_molecular_test: Optional[str] = None
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            test_analyte_type: Optional[str] = None
            test_result: Optional[str] = None
            test_units: Optional[str] = None
            test_value: Optional[float] = None
            transcript: Optional[str] = None
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
            variant_origin: Optional[str] = None
            variant_type: Optional[str] = None
            zygosity: Optional[str] = None

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.aa_change == self.aa_change
                assert row.antigen == self.antigen
                assert row.biospecimen_type == self.biospecimen_type
                assert row.biospecimen_volume == self.biospecimen_volume
                assert (
                    row.blood_test_normal_range_lower
                    == self.blood_test_normal_range_lower
                )
                assert (
                    row.blood_test_normal_range_upper
                    == self.blood_test_normal_range_upper
                )
                assert row.cell_count == self.cell_count
                assert row.chromosome == self.chromosome
                assert row.clonality == self.clonality
                assert row.copy_number == self.copy_number
                assert row.created_datetime == self.created_datetime
                assert row.cytoband == self.cytoband
                assert row.days_to_test == self.days_to_test
                assert row.exon == self.exon
                assert row.gene_symbol == self.gene_symbol
                assert row.histone_family == self.histone_family
                assert row.histone_variant == self.histone_variant
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
                assert row.pathogenicity == self.pathogenicity
                assert row.ploidy == self.ploidy
                assert row.second_exon == self.second_exon
                assert row.second_gene_symbol == self.second_gene_symbol
                assert row.specialized_molecular_test == self.specialized_molecular_test
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.test_analyte_type == self.test_analyte_type
                assert row.test_result == self.test_result
                assert row.test_units == self.test_units
                assert row.test_value == self.test_value
                assert row.transcript == self.transcript
                assert row.updated_datetime == self.updated_datetime
                assert row.variant_origin == self.variant_origin
                assert row.variant_type == self.variant_type
                assert row.zygosity == self.zygosity

                return True

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
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        days_to_adverse_event: Optional[int] = None
        days_to_comorbidity: Optional[int] = None
        days_to_follow_up: Optional[int] = None
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
        follow_up_id: Optional[str] = None
        haart_treatment_indicator: Optional[str] = None
        height: Optional[float] = None
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
        molecular_tests: Optional[tuple[MolecularTest, ...]] = (MolecularTest(),)
        nadir_cd4_count: Optional[float] = None
        pancreatitis_onset_year: Optional[int] = None
        pregnancy_outcome: Optional[str] = None
        procedures_performed: Optional[str] = None
        progression_or_recurrence: Optional[str] = "not reported"
        progression_or_recurrence_anatomic_site: Optional[str] = None
        progression_or_recurrence_type: Optional[str] = None
        recist_targeted_regions_number: Optional[int] = None
        recist_targeted_regions_sum: Optional[float] = None
        reflux_treatment_type: Optional[str] = None
        risk_factor: Optional[str] = None
        risk_factor_treatment: Optional[str] = None
        scan_tracer_used: Optional[str] = None
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        undescended_testis_corrected: Optional[str] = None
        undescended_testis_corrected_age: Optional[int] = None
        undescended_testis_corrected_laterality: Optional[str] = None
        undescended_testis_corrected_method: Optional[str] = None
        undescended_testis_history: Optional[str] = None
        undescended_testis_history_laterality: Optional[str] = None
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
        viral_hepatitis_serologies: Optional[str] = None
        weight: Optional[float] = 20.0

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.adverse_event == self.adverse_event
            assert row.adverse_event_grade == self.adverse_event_grade
            assert row.aids_risk_factors == self.aids_risk_factors
            assert (
                row.barretts_esophagus_goblet_cells_present
                == self.barretts_esophagus_goblet_cells_present
            )
            assert row.bmi == self.bmi
            assert row.body_surface_area == self.body_surface_area
            assert row.cause_of_response == self.cause_of_response
            assert row.cd4_count == self.cd4_count
            assert row.cdc_hiv_risk_factors == self.cdc_hiv_risk_factors
            assert row.comorbidity == self.comorbidity
            assert (
                row.comorbidity_method_of_diagnosis
                == self.comorbidity_method_of_diagnosis
            )
            assert row.created_datetime == self.created_datetime
            assert row.days_to_adverse_event == self.days_to_adverse_event
            assert row.days_to_comorbidity == self.days_to_comorbidity
            assert row.days_to_follow_up == self.days_to_follow_up
            assert row.days_to_imaging == self.days_to_imaging
            assert row.days_to_progression == self.days_to_progression
            assert row.days_to_progression_free == self.days_to_progression_free
            assert row.days_to_recurrence == self.days_to_recurrence
            assert row.diabetes_treatment_type == self.diabetes_treatment_type
            assert row.disease_response == self.disease_response
            assert row.dlco_ref_predictive_percent == self.dlco_ref_predictive_percent
            assert row.ecog_performance_status == self.ecog_performance_status
            assert row.evidence_of_recurrence_type == self.evidence_of_recurrence_type
            assert row.eye_color == self.eye_color
            assert row.fev1_fvc_post_bronch_percent == self.fev1_fvc_post_bronch_percent
            assert row.fev1_fvc_pre_bronch_percent == self.fev1_fvc_pre_bronch_percent
            assert row.fev1_ref_post_bronch_percent == self.fev1_ref_post_bronch_percent
            assert row.fev1_ref_pre_bronch_percent == self.fev1_ref_pre_bronch_percent
            assert row.follow_up_id == self.follow_up_id
            assert row.haart_treatment_indicator == self.haart_treatment_indicator
            assert row.height == self.height
            assert (
                row.hepatitis_sustained_virological_response
                == self.hepatitis_sustained_virological_response
            )
            assert row.history_of_tumor == self.history_of_tumor
            assert row.history_of_tumor_type == self.history_of_tumor_type
            assert row.hiv_viral_load == self.hiv_viral_load
            assert row.hormonal_contraceptive_type == self.hormonal_contraceptive_type
            assert row.hormonal_contraceptive_use == self.hormonal_contraceptive_use
            assert (
                row.hormone_replacement_therapy_type
                == self.hormone_replacement_therapy_type
            )
            assert row.hpv_positive_type == self.hpv_positive_type
            assert (
                row.hysterectomy_margins_involved == self.hysterectomy_margins_involved
            )
            assert row.hysterectomy_type == self.hysterectomy_type
            assert row.imaging_result == self.imaging_result
            assert row.imaging_type == self.imaging_type
            assert (
                row.immunosuppressive_treatment_type
                == self.immunosuppressive_treatment_type
            )
            assert row.karnofsky_performance_status == self.karnofsky_performance_status
            assert row.menopause_status == self.menopause_status
            assert row.nadir_cd4_count == self.nadir_cd4_count
            assert row.pancreatitis_onset_year == self.pancreatitis_onset_year
            assert row.pregnancy_outcome == self.pregnancy_outcome
            assert row.procedures_performed == self.procedures_performed
            assert row.progression_or_recurrence == self.progression_or_recurrence
            assert (
                row.progression_or_recurrence_anatomic_site
                == self.progression_or_recurrence_anatomic_site
            )
            assert (
                row.progression_or_recurrence_type
                == self.progression_or_recurrence_type
            )
            assert (
                row.recist_targeted_regions_number
                == self.recist_targeted_regions_number
            )
            assert row.recist_targeted_regions_sum == self.recist_targeted_regions_sum
            assert row.reflux_treatment_type == self.reflux_treatment_type
            assert row.risk_factor == self.risk_factor
            assert row.risk_factor_treatment == self.risk_factor_treatment
            assert row.scan_tracer_used == self.scan_tracer_used
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.undescended_testis_corrected == self.undescended_testis_corrected
            assert (
                row.undescended_testis_corrected_age
                == self.undescended_testis_corrected_age
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
            assert row.updated_datetime == self.updated_datetime
            assert row.viral_hepatitis_serologies == self.viral_hepatitis_serologies
            assert row.weight == self.weight
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.molecular_tests or (), self.molecular_tests or ())
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Project:
        @dataclasses.dataclass(frozen=True)
        class Program:
            dbgap_accession_number: Optional[str] = "phs000178"
            name: Optional[str] = "MD Anderson - RPPA Core Facility (Proteomics)"
            program_id: Optional[str] = "program-0"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.dbgap_accession_number == self.dbgap_accession_number
                assert row.name == self.name
                assert row.program_id == self.program_id

                return True

        dbgap_accession_number: Optional[str] = "phs000178"
        disease_type: Optional[tuple[str, ...]] = ("Ductal and Lobular Neoplasms",)
        intended_release_date: Optional[str] = None
        name: Optional[str] = "MD Anderson - RPPA Core Facility (Proteomics)"
        primary_site: Optional[tuple[str, ...]] = ("Breast",)
        program: Optional[Program] = Program()
        project_id: Optional[str] = "GDC-TEST"
        releasable: Optional[str] = "True"
        released: Optional[str] = "True"
        state: Optional[str] = "released"

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
        @dataclasses.dataclass(frozen=True)
        class Annotation:
            annotation_id: Optional[str] = "annotation-0"
            case_id: Optional[str] = "case-0"
            case_submitter_id: Optional[str] = None
            category: Optional[str] = "General"
            classification: Optional[str] = "Observation"
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            creator: Optional[str] = None
            entity_id: Optional[str] = "entity-0"
            entity_submitter_id: Optional[str] = "sub-entity-0"
            entity_type: Optional[str] = "aliquot"
            legacy_created_datetime: Optional[str] = None
            legacy_updated_datetime: Optional[str] = None
            notes: Optional[
                str
            ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
            state: Optional[str] = "released"
            status: Optional[str] = "Approved"
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.annotation_id == self.annotation_id
                assert row.case_id == self.case_id
                assert row.case_submitter_id == self.case_submitter_id
                assert row.category == self.category
                assert row.classification == self.classification
                assert row.created_datetime == self.created_datetime
                assert row.creator == self.creator
                assert row.entity_id == self.entity_id
                assert row.entity_submitter_id == self.entity_submitter_id
                assert row.entity_type == self.entity_type
                assert row.legacy_created_datetime == self.legacy_created_datetime
                assert row.legacy_updated_datetime == self.legacy_updated_datetime
                assert row.notes == self.notes
                assert row.state == self.state
                assert row.status == self.status
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime

                return True

        @dataclasses.dataclass(frozen=True)
        class Portion:
            @dataclasses.dataclass(frozen=True)
            class Analyte:
                @dataclasses.dataclass(frozen=True)
                class Aliquot:
                    @dataclasses.dataclass(frozen=True)
                    class Annotation:
                        annotation_id: Optional[str] = "annotation-0"
                        case_id: Optional[str] = "case-0"
                        case_submitter_id: Optional[str] = None
                        category: Optional[str] = "General"
                        classification: Optional[str] = "Observation"
                        created_datetime: Optional[
                            str
                        ] = "2018-05-21T16:07:40.645885-05:00"
                        creator: Optional[str] = None
                        entity_id: Optional[str] = "entity-0"
                        entity_submitter_id: Optional[str] = "sub-entity-0"
                        entity_type: Optional[str] = "aliquot"
                        legacy_created_datetime: Optional[str] = None
                        legacy_updated_datetime: Optional[str] = None
                        notes: Optional[
                            str
                        ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                        state: Optional[str] = "released"
                        status: Optional[str] = "Approved"
                        submitter_id: Optional[str] = "sub-generic-0"
                        updated_datetime: Optional[
                            str
                        ] = "2018-11-01T15:06:10.843096-05:00"

                        def assert_equals(self, row: sql.Row) -> bool:
                            assert row
                            assert row.annotation_id == self.annotation_id
                            assert row.case_id == self.case_id
                            assert row.case_submitter_id == self.case_submitter_id
                            assert row.category == self.category
                            assert row.classification == self.classification
                            assert row.created_datetime == self.created_datetime
                            assert row.creator == self.creator
                            assert row.entity_id == self.entity_id
                            assert row.entity_submitter_id == self.entity_submitter_id
                            assert row.entity_type == self.entity_type
                            assert (
                                row.legacy_created_datetime
                                == self.legacy_created_datetime
                            )
                            assert (
                                row.legacy_updated_datetime
                                == self.legacy_updated_datetime
                            )
                            assert row.notes == self.notes
                            assert row.state == self.state
                            assert row.status == self.status
                            assert row.submitter_id == self.submitter_id
                            assert row.updated_datetime == self.updated_datetime

                            return True

                    @dataclasses.dataclass(frozen=True)
                    class Center:
                        center_id: Optional[str] = "center-0"
                        center_type: Optional[str] = "CGCC"
                        code: Optional[str] = "20"
                        name: Optional[
                            str
                        ] = "MD Anderson - RPPA Core Facility (Proteomics)"
                        namespace: Optional[str] = "mdanderson.org"
                        short_name: Optional[str] = "MDA"

                        def assert_equals(self, row: sql.Row) -> bool:
                            assert row
                            assert row.center_id == self.center_id
                            assert row.center_type == self.center_type
                            assert row.code == self.code
                            assert row.name == self.name
                            assert row.namespace == self.namespace
                            assert row.short_name == self.short_name

                            return True

                    aliquot_id: Optional[str] = "aliquot-0"
                    aliquot_quantity: Optional[float] = 4.54
                    aliquot_volume: Optional[float] = 26.7
                    amount: Optional[float] = None
                    analyte_type: Optional[str] = "RNA"
                    analyte_type_id: Optional[str] = "R"
                    annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
                    center: Optional[Center] = Center()
                    concentration: Optional[float] = 0.17
                    created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                    no_matched_normal_low_pass_wgs: Optional[str] = None
                    no_matched_normal_targeted_sequencing: Optional[str] = None
                    no_matched_normal_wgs: Optional[str] = None
                    no_matched_normal_wxs: Optional[str] = None
                    selected_normal_low_pass_wgs: Optional[str] = None
                    selected_normal_targeted_sequencing: Optional[str] = None
                    selected_normal_wgs: Optional[str] = "True"
                    selected_normal_wxs: Optional[str] = "True"
                    source_center: Optional[str] = "23"
                    state: Optional[str] = "released"
                    submitter_id: Optional[str] = "sub-generic-0"
                    updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                    def assert_equals(self, row: sql.Row) -> bool:
                        assert row
                        assert row.aliquot_id == self.aliquot_id
                        assert row.aliquot_quantity == self.aliquot_quantity
                        assert row.aliquot_volume == self.aliquot_volume
                        assert row.amount == self.amount
                        assert row.analyte_type == self.analyte_type
                        assert row.analyte_type_id == self.analyte_type_id
                        assert row.concentration == self.concentration
                        assert row.created_datetime == self.created_datetime
                        assert (
                            row.no_matched_normal_low_pass_wgs
                            == self.no_matched_normal_low_pass_wgs
                        )
                        assert (
                            row.no_matched_normal_targeted_sequencing
                            == self.no_matched_normal_targeted_sequencing
                        )
                        assert row.no_matched_normal_wgs == self.no_matched_normal_wgs
                        assert row.no_matched_normal_wxs == self.no_matched_normal_wxs
                        assert (
                            row.selected_normal_low_pass_wgs
                            == self.selected_normal_low_pass_wgs
                        )
                        assert (
                            row.selected_normal_targeted_sequencing
                            == self.selected_normal_targeted_sequencing
                        )
                        assert row.selected_normal_wgs == self.selected_normal_wgs
                        assert row.selected_normal_wxs == self.selected_normal_wxs
                        assert row.source_center == self.source_center
                        assert row.state == self.state
                        assert row.submitter_id == self.submitter_id
                        assert row.updated_datetime == self.updated_datetime
                        assert (row.center is None and self.center is None) or (
                            self.center and self.center.assert_equals(row.center)
                        )
                        assert all(
                            e.assert_equals(r)
                            for r, e in zip(
                                row.annotations or (), self.annotations or ()
                            )
                        )

                        return True

                @dataclasses.dataclass(frozen=True)
                class Annotation:
                    annotation_id: Optional[str] = "annotation-0"
                    case_id: Optional[str] = "case-0"
                    case_submitter_id: Optional[str] = None
                    category: Optional[str] = "General"
                    classification: Optional[str] = "Observation"
                    created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                    creator: Optional[str] = None
                    entity_id: Optional[str] = "entity-0"
                    entity_submitter_id: Optional[str] = "sub-entity-0"
                    entity_type: Optional[str] = "aliquot"
                    legacy_created_datetime: Optional[str] = None
                    legacy_updated_datetime: Optional[str] = None
                    notes: Optional[
                        str
                    ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                    state: Optional[str] = "released"
                    status: Optional[str] = "Approved"
                    submitter_id: Optional[str] = "sub-generic-0"
                    updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                    def assert_equals(self, row: sql.Row) -> bool:
                        assert row
                        assert row.annotation_id == self.annotation_id
                        assert row.case_id == self.case_id
                        assert row.case_submitter_id == self.case_submitter_id
                        assert row.category == self.category
                        assert row.classification == self.classification
                        assert row.created_datetime == self.created_datetime
                        assert row.creator == self.creator
                        assert row.entity_id == self.entity_id
                        assert row.entity_submitter_id == self.entity_submitter_id
                        assert row.entity_type == self.entity_type
                        assert (
                            row.legacy_created_datetime == self.legacy_created_datetime
                        )
                        assert (
                            row.legacy_updated_datetime == self.legacy_updated_datetime
                        )
                        assert row.notes == self.notes
                        assert row.state == self.state
                        assert row.status == self.status
                        assert row.submitter_id == self.submitter_id
                        assert row.updated_datetime == self.updated_datetime

                        return True

                a260_a280_ratio: Optional[float] = 1.78
                aliquots: Optional[tuple[Aliquot, ...]] = (Aliquot(),)
                amount: Optional[float] = None
                analyte_id: Optional[str] = "analyte-0"
                analyte_quantity: Optional[float] = None
                analyte_type: Optional[str] = "RNA"
                analyte_type_id: Optional[str] = "R"
                analyte_volume: Optional[float] = None
                annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
                concentration: Optional[float] = 0.17
                created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                experimental_protocol_type: Optional[str] = "mirVana (Allprep DNA) RNA"
                normal_tumor_genotype_snp_match: Optional[str] = "Yes"
                ribosomal_rna_28s_16s_ratio: Optional[float] = 1.7
                rna_integrity_number: Optional[float] = 8.1
                spectrophotometer_method: Optional[str] = "UV Spec"
                state: Optional[str] = "released"
                submitter_id: Optional[str] = "sub-generic-0"
                updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
                well_number: Optional[str] = None

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.a260_a280_ratio == self.a260_a280_ratio
                    assert row.amount == self.amount
                    assert row.analyte_id == self.analyte_id
                    assert row.analyte_quantity == self.analyte_quantity
                    assert row.analyte_type == self.analyte_type
                    assert row.analyte_type_id == self.analyte_type_id
                    assert row.analyte_volume == self.analyte_volume
                    assert row.concentration == self.concentration
                    assert row.created_datetime == self.created_datetime
                    assert (
                        row.experimental_protocol_type
                        == self.experimental_protocol_type
                    )
                    assert (
                        row.normal_tumor_genotype_snp_match
                        == self.normal_tumor_genotype_snp_match
                    )
                    assert (
                        row.ribosomal_rna_28s_16s_ratio
                        == self.ribosomal_rna_28s_16s_ratio
                    )
                    assert row.rna_integrity_number == self.rna_integrity_number
                    assert row.spectrophotometer_method == self.spectrophotometer_method
                    assert row.state == self.state
                    assert row.submitter_id == self.submitter_id
                    assert row.updated_datetime == self.updated_datetime
                    assert row.well_number == self.well_number
                    assert all(
                        e.assert_equals(r)
                        for r, e in zip(row.aliquots or (), self.aliquots or ())
                    )
                    assert all(
                        e.assert_equals(r)
                        for r, e in zip(row.annotations or (), self.annotations or ())
                    )

                    return True

            @dataclasses.dataclass(frozen=True)
            class Annotation:
                annotation_id: Optional[str] = "annotation-0"
                case_id: Optional[str] = "case-0"
                case_submitter_id: Optional[str] = None
                category: Optional[str] = "General"
                classification: Optional[str] = "Observation"
                created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                creator: Optional[str] = None
                entity_id: Optional[str] = "entity-0"
                entity_submitter_id: Optional[str] = "sub-entity-0"
                entity_type: Optional[str] = "aliquot"
                legacy_created_datetime: Optional[str] = None
                legacy_updated_datetime: Optional[str] = None
                notes: Optional[
                    str
                ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                state: Optional[str] = "released"
                status: Optional[str] = "Approved"
                submitter_id: Optional[str] = "sub-generic-0"
                updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.annotation_id == self.annotation_id
                    assert row.case_id == self.case_id
                    assert row.case_submitter_id == self.case_submitter_id
                    assert row.category == self.category
                    assert row.classification == self.classification
                    assert row.created_datetime == self.created_datetime
                    assert row.creator == self.creator
                    assert row.entity_id == self.entity_id
                    assert row.entity_submitter_id == self.entity_submitter_id
                    assert row.entity_type == self.entity_type
                    assert row.legacy_created_datetime == self.legacy_created_datetime
                    assert row.legacy_updated_datetime == self.legacy_updated_datetime
                    assert row.notes == self.notes
                    assert row.state == self.state
                    assert row.status == self.status
                    assert row.submitter_id == self.submitter_id
                    assert row.updated_datetime == self.updated_datetime

                    return True

            @dataclasses.dataclass(frozen=True)
            class Center:
                center_id: Optional[str] = "center-0"
                center_type: Optional[str] = "CGCC"
                code: Optional[str] = "20"
                name: Optional[str] = "MD Anderson - RPPA Core Facility (Proteomics)"
                namespace: Optional[str] = "mdanderson.org"
                short_name: Optional[str] = "MDA"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.center_id == self.center_id
                    assert row.center_type == self.center_type
                    assert row.code == self.code
                    assert row.name == self.name
                    assert row.namespace == self.namespace
                    assert row.short_name == self.short_name

                    return True

            @dataclasses.dataclass(frozen=True)
            class Slide:
                @dataclasses.dataclass(frozen=True)
                class Annotation:
                    annotation_id: Optional[str] = "annotation-0"
                    case_id: Optional[str] = "case-0"
                    case_submitter_id: Optional[str] = None
                    category: Optional[str] = "General"
                    classification: Optional[str] = "Observation"
                    created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                    creator: Optional[str] = None
                    entity_id: Optional[str] = "entity-0"
                    entity_submitter_id: Optional[str] = "sub-entity-0"
                    entity_type: Optional[str] = "aliquot"
                    legacy_created_datetime: Optional[str] = None
                    legacy_updated_datetime: Optional[str] = None
                    notes: Optional[
                        str
                    ] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                    state: Optional[str] = "released"
                    status: Optional[str] = "Approved"
                    submitter_id: Optional[str] = "sub-generic-0"
                    updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                    def assert_equals(self, row: sql.Row) -> bool:
                        assert row
                        assert row.annotation_id == self.annotation_id
                        assert row.case_id == self.case_id
                        assert row.case_submitter_id == self.case_submitter_id
                        assert row.category == self.category
                        assert row.classification == self.classification
                        assert row.created_datetime == self.created_datetime
                        assert row.creator == self.creator
                        assert row.entity_id == self.entity_id
                        assert row.entity_submitter_id == self.entity_submitter_id
                        assert row.entity_type == self.entity_type
                        assert (
                            row.legacy_created_datetime == self.legacy_created_datetime
                        )
                        assert (
                            row.legacy_updated_datetime == self.legacy_updated_datetime
                        )
                        assert row.notes == self.notes
                        assert row.state == self.state
                        assert row.status == self.status
                        assert row.submitter_id == self.submitter_id
                        assert row.updated_datetime == self.updated_datetime

                        return True

                annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
                bone_marrow_malignant_cells: Optional[str] = None
                created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                number_proliferating_cells: Optional[int] = None
                percent_eosinophil_infiltration: Optional[float] = None
                percent_follicular_component: Optional[float] = None
                percent_granulocyte_infiltration: Optional[float] = None
                percent_inflam_infiltration: Optional[float] = None
                percent_lymphocyte_infiltration: Optional[float] = 0.0
                percent_monocyte_infiltration: Optional[float] = 0.0
                percent_necrosis: Optional[float] = 10.0
                percent_neutrophil_infiltration: Optional[float] = 0.0
                percent_normal_cells: Optional[float] = 0.0
                percent_rhabdoid_features: Optional[float] = None
                percent_sarcomatoid_features: Optional[float] = None
                percent_stromal_cells: Optional[float] = 40.0
                percent_tumor_cells: Optional[float] = 50.0
                percent_tumor_nuclei: Optional[float] = 70.0
                prostatic_chips_positive_count: Optional[float] = None
                prostatic_chips_total_count: Optional[float] = None
                prostatic_involvement_percent: Optional[float] = None
                section_location: Optional[str] = "TOP"
                slide_id: Optional[str] = "slide-0"
                state: Optional[str] = "released"
                submitter_id: Optional[str] = "sub-generic-0"
                tissue_microarray_coordinates: Optional[str] = None
                updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert (
                        row.bone_marrow_malignant_cells
                        == self.bone_marrow_malignant_cells
                    )
                    assert row.created_datetime == self.created_datetime
                    assert (
                        row.number_proliferating_cells
                        == self.number_proliferating_cells
                    )
                    assert (
                        row.percent_eosinophil_infiltration
                        == self.percent_eosinophil_infiltration
                    )
                    assert (
                        row.percent_follicular_component
                        == self.percent_follicular_component
                    )
                    assert (
                        row.percent_granulocyte_infiltration
                        == self.percent_granulocyte_infiltration
                    )
                    assert (
                        row.percent_inflam_infiltration
                        == self.percent_inflam_infiltration
                    )
                    assert (
                        row.percent_lymphocyte_infiltration
                        == self.percent_lymphocyte_infiltration
                    )
                    assert (
                        row.percent_monocyte_infiltration
                        == self.percent_monocyte_infiltration
                    )
                    assert row.percent_necrosis == self.percent_necrosis
                    assert (
                        row.percent_neutrophil_infiltration
                        == self.percent_neutrophil_infiltration
                    )
                    assert row.percent_normal_cells == self.percent_normal_cells
                    assert (
                        row.percent_rhabdoid_features == self.percent_rhabdoid_features
                    )
                    assert (
                        row.percent_sarcomatoid_features
                        == self.percent_sarcomatoid_features
                    )
                    assert row.percent_stromal_cells == self.percent_stromal_cells
                    assert row.percent_tumor_cells == self.percent_tumor_cells
                    assert row.percent_tumor_nuclei == self.percent_tumor_nuclei
                    assert (
                        row.prostatic_chips_positive_count
                        == self.prostatic_chips_positive_count
                    )
                    assert (
                        row.prostatic_chips_total_count
                        == self.prostatic_chips_total_count
                    )
                    assert (
                        row.prostatic_involvement_percent
                        == self.prostatic_involvement_percent
                    )
                    assert row.section_location == self.section_location
                    assert row.slide_id == self.slide_id
                    assert row.state == self.state
                    assert row.submitter_id == self.submitter_id
                    assert (
                        row.tissue_microarray_coordinates
                        == self.tissue_microarray_coordinates
                    )
                    assert row.updated_datetime == self.updated_datetime
                    assert all(
                        e.assert_equals(r)
                        for r, e in zip(row.annotations or (), self.annotations or ())
                    )

                    return True

            analytes: Optional[tuple[Analyte, ...]] = (Analyte(),)
            annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
            center: Optional[Center] = Center()
            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
            creation_datetime: Optional[float] = 1311120000.0
            is_ffpe: Optional[str] = None
            portion_id: Optional[str] = "portion-0"
            portion_number: Optional[str] = "21"
            slides: Optional[tuple[Slide, ...]] = (Slide(),)
            state: Optional[str] = "released"
            submitter_id: Optional[str] = "sub-generic-0"
            updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"
            weight: Optional[float] = 20.0

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.created_datetime == self.created_datetime
                assert row.creation_datetime == self.creation_datetime
                assert row.is_ffpe == self.is_ffpe
                assert row.portion_id == self.portion_id
                assert row.portion_number == self.portion_number
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.updated_datetime == self.updated_datetime
                assert row.weight == self.weight
                assert (row.center is None and self.center is None) or (
                    self.center and self.center.assert_equals(row.center)
                )
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.analytes or (), self.analytes or ())
                )
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.annotations or (), self.annotations or ())
                )
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.slides or (), self.slides or ())
                )

                return True

        annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
        biospecimen_anatomic_site: Optional[str] = None
        biospecimen_laterality: Optional[str] = None
        catalog_reference: Optional[str] = None
        composition: Optional[str] = "Not Reported"
        created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
        current_weight: Optional[float] = None
        days_to_collection: Optional[int] = 62
        days_to_sample_procurement: Optional[int] = 0
        diagnosis_pathologically_confirmed: Optional[str] = None
        distance_normal_to_tumor: Optional[str] = None
        distributor_reference: Optional[str] = None
        freezing_method: Optional[str] = None
        growth_rate: Optional[int] = None
        initial_weight: Optional[float] = 200.0
        intermediate_dimension: Optional[float] = None
        is_ffpe: Optional[str] = None
        longest_dimension: Optional[float] = None
        method_of_sample_procurement: Optional[str] = None
        oct_embedded: Optional[str] = "true"
        passage_count: Optional[int] = None
        pathology_report_uuid: Optional[str] = "57323AE5-3EFE-4492-8522-D9A6DB3F1BE0"
        portions: Optional[tuple[Portion, ...]] = (Portion(),)
        preservation_method: Optional[str] = "FFPE"
        sample_id: Optional[str] = "sample-0"
        sample_ordinal: Optional[int] = None
        sample_type: Optional[str] = "Primary Tumor"
        sample_type_id: Optional[str] = "01"
        shortest_dimension: Optional[float] = None
        specimen_type: Optional[str] = "Unknown"
        state: Optional[str] = "released"
        submitter_id: Optional[str] = "sub-generic-0"
        time_between_clamping_and_freezing: Optional[float] = None
        time_between_excision_and_freezing: Optional[float] = None
        tissue_collection_type: Optional[str] = None
        tissue_type: Optional[str] = "Not Reported"
        tumor_code: Optional[str] = None
        tumor_code_id: Optional[str] = None
        tumor_descriptor: Optional[str] = "Not Reported"
        updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.biospecimen_anatomic_site == self.biospecimen_anatomic_site
            assert row.biospecimen_laterality == self.biospecimen_laterality
            assert row.catalog_reference == self.catalog_reference
            assert row.composition == self.composition
            assert row.created_datetime == self.created_datetime
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
            assert row.is_ffpe == self.is_ffpe
            assert row.longest_dimension == self.longest_dimension
            assert row.method_of_sample_procurement == self.method_of_sample_procurement
            assert row.oct_embedded == self.oct_embedded
            assert row.passage_count == self.passage_count
            assert row.pathology_report_uuid == self.pathology_report_uuid
            assert row.preservation_method == self.preservation_method
            assert row.sample_id == self.sample_id
            assert row.sample_ordinal == self.sample_ordinal
            assert row.sample_type == self.sample_type
            assert row.sample_type_id == self.sample_type_id
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
            assert row.tumor_code == self.tumor_code
            assert row.tumor_code_id == self.tumor_code_id
            assert row.tumor_descriptor == self.tumor_descriptor
            assert row.updated_datetime == self.updated_datetime
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.annotations or (), self.annotations or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.portions or (), self.portions or ())
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Summary:
        @dataclasses.dataclass(frozen=True)
        class DataCategory:
            data_category: Optional[str] = "Biospecimen"
            file_count: Optional[int] = 1

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.data_category == self.data_category
                assert row.file_count == self.file_count

                return True

        @dataclasses.dataclass(frozen=True)
        class ExperimentalStrategy:
            experimental_strategy: Optional[str] = "WXS"
            file_count: Optional[int] = 1

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.experimental_strategy == self.experimental_strategy
                assert row.file_count == self.file_count

                return True

        data_categories: Optional[tuple[DataCategory, ...]] = (DataCategory(),)
        experimental_strategies: Optional[tuple[ExperimentalStrategy, ...]] = (
            ExperimentalStrategy(),
        )
        file_count: Optional[int] = 1
        file_size: Optional[int] = 353327

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.file_count == self.file_count
            assert row.file_size == self.file_size
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.data_categories or (), self.data_categories or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(
                    row.experimental_strategies or (),
                    self.experimental_strategies or (),
                )
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class TissueSourceSite:
        bcr_id: Optional[str] = "NCH"
        code: Optional[str] = "20"
        name: Optional[str] = "MD Anderson - RPPA Core Facility (Proteomics)"
        project: Optional[str] = "Breast invasive carcinoma"
        tissue_source_site_id: Optional[str] = "tissue-source-site-0"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.bcr_id == self.bcr_id
            assert row.code == self.code
            assert row.name == self.name
            assert row.project == self.project
            assert row.tissue_source_site_id == self.tissue_source_site_id

            return True

    aliquot_ids: Optional[tuple[str, ...]] = ("aliquot-0",)
    analyte_ids: Optional[tuple[str, ...]] = ("analyte-0",)
    annotations: Optional[tuple[Annotation, ...]] = (Annotation(),)
    case_id: Optional[str] = "case-0"
    consent_type: Optional[str] = None
    created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
    days_to_consent: Optional[int] = None
    days_to_lost_to_followup: Optional[int] = None
    demographic: Optional[Demographic] = Demographic()
    diagnoses: Optional[tuple[Diagnosis, ...]] = (Diagnosis(),)
    diagnosis_ids: Optional[tuple[str, ...]] = ("diagnosis-0",)
    disease_type: Optional[str] = "Ductal and Lobular Neoplasms"
    exposures: Optional[tuple[Exposure, ...]] = (Exposure(),)
    family_histories: Optional[tuple[FamilyHistory, ...]] = (FamilyHistory(),)
    files: Optional[tuple[File, ...]] = (File(),)
    follow_ups: Optional[tuple[FollowUp, ...]] = (FollowUp(),)
    index_date: Optional[str] = None
    lost_to_followup: Optional[str] = None
    portion_ids: Optional[tuple[str, ...]] = ("portion-0",)
    primary_site: Optional[str] = "Breast"
    project: Optional[Project] = Project()
    sample_ids: Optional[tuple[str, ...]] = ("sample-0",)
    samples: tuple[Sample, ...] = (Sample(),)
    slide_ids: Optional[tuple[str, ...]] = ("slide-0",)
    state: Optional[str] = "released"
    submitter_aliquot_ids: Optional[tuple[str, ...]] = ("sub-aliquot-0",)
    submitter_analyte_ids: Optional[tuple[str, ...]] = ("sub-analyte-0",)
    submitter_diagnosis_ids: Optional[tuple[str, ...]] = ("sub-diagnosis-0",)
    submitter_id: Optional[str] = "sub-generic-0"
    submitter_portion_ids: Optional[tuple[str, ...]] = ("sub-portion-0",)
    submitter_sample_ids: Optional[tuple[str, ...]] = ("sub-sample-0",)
    submitter_slide_ids: Optional[tuple[str, ...]] = ("sub-slide-0",)
    summary: Optional[Summary] = Summary()
    tissue_source_site: Optional[TissueSourceSite] = TissueSourceSite()
    updated_datetime: Optional[str] = "2018-11-01T15:06:10.843096-05:00"

    def assert_equals(self, row: sql.Row) -> bool:
        assert row
        assert row.case_id == self.case_id
        assert row.consent_type == self.consent_type
        assert row.created_datetime == self.created_datetime
        assert row.days_to_consent == self.days_to_consent
        assert row.days_to_lost_to_followup == self.days_to_lost_to_followup
        assert row.disease_type == self.disease_type
        assert row.index_date == self.index_date
        assert row.lost_to_followup == self.lost_to_followup
        assert row.primary_site == self.primary_site
        assert row.state == self.state
        assert row.submitter_id == self.submitter_id
        assert row.updated_datetime == self.updated_datetime
        assert tuple(row.aliquot_ids) == self.aliquot_ids
        assert tuple(row.analyte_ids) == self.analyte_ids
        assert tuple(row.diagnosis_ids) == self.diagnosis_ids
        assert tuple(row.portion_ids) == self.portion_ids
        assert tuple(row.sample_ids) == self.sample_ids
        assert tuple(row.slide_ids) == self.slide_ids
        assert tuple(row.submitter_aliquot_ids) == self.submitter_aliquot_ids
        assert tuple(row.submitter_analyte_ids) == self.submitter_analyte_ids
        assert tuple(row.submitter_diagnosis_ids) == self.submitter_diagnosis_ids
        assert tuple(row.submitter_portion_ids) == self.submitter_portion_ids
        assert tuple(row.submitter_sample_ids) == self.submitter_sample_ids
        assert tuple(row.submitter_slide_ids) == self.submitter_slide_ids
        assert (row.demographic is None and self.demographic is None) or (
            self.demographic and self.demographic.assert_equals(row.demographic)
        )
        assert (row.project is None and self.project is None) or (
            self.project and self.project.assert_equals(row.project)
        )
        assert all(s.assert_equals(rs) for rs, s in zip(row.samples, self.samples))
        assert (row.summary is None and self.summary is None) or (
            self.summary and self.summary.assert_equals(row.summary)
        )
        assert (row.tissue_source_site is None and self.tissue_source_site is None) or (
            self.tissue_source_site
            and self.tissue_source_site.assert_equals(row.tissue_source_site)
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.annotations or (), self.annotations or ())
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.diagnoses or (), self.diagnoses or ())
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.exposures or (), self.exposures or ())
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.family_histories or (), self.family_histories or ())
        )
        assert all(
            e.assert_equals(r) for r, e in zip(row.files or (), self.files or ())
        )
        assert all(
            e.assert_equals(r)
            for r, e in zip(row.follow_ups or (), self.follow_ups or ())
        )

        return True
