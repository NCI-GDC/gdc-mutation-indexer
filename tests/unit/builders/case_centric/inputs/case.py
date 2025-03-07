import dataclasses

from pyspark import sql


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Annotation:
        annotation_id: str | None = "annotation-0"
        case_id: str | None = "case-0"
        case_submitter_id: str | None = None
        category: str | None = "General"
        classification: str | None = "Observation"
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        creator: str | None = None
        entity_id: str | None = "entity-0"
        entity_submitter_id: str | None = "sub-entity-0"
        entity_type: str | None = "aliquot"
        legacy_created_datetime: str | None = None
        legacy_updated_datetime: str | None = None
        notes: None | (
            str
        ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
        state: str | None = "released"
        status: str | None = "Approved"
        submitter_id: str | None = "sub-generic-0"
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
        age_at_index: int | None = 60
        age_is_obfuscated: str | None = None
        cause_of_death: str | None = None
        cause_of_death_source: str | None = None
        country_of_residence_at_enrollment: str | None = None
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        days_to_birth: int | None = -22052
        days_to_death: int | None = 1324
        demographic_id: str | None = "demographic-0"
        ethnicity: str | None = "not hispanic or latino"
        gender: str | None = "female"
        occupation_duration_years: int | None = None
        premature_at_birth: str | None = None
        race: str | None = "white"
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
        vital_status: str | None = "Alive"
        weeks_gestation_at_birth: float | None = None
        year_of_birth: int | None = 1951
        year_of_death: int | None = 2009

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
            annotation_id: str | None = "annotation-0"
            case_id: str | None = "case-0"
            case_submitter_id: str | None = None
            category: str | None = "General"
            classification: str | None = "Observation"
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            creator: str | None = None
            entity_id: str | None = "entity-0"
            entity_submitter_id: str | None = "sub-entity-0"
            entity_type: str | None = "aliquot"
            legacy_created_datetime: str | None = None
            legacy_updated_datetime: str | None = None
            notes: None | (
                str
            ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
            state: str | None = "released"
            status: str | None = "Approved"
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
            additional_pathology_findings: str | None = None
            anaplasia_present: str | None = None
            anaplasia_present_type: str | None = None
            bone_marrow_malignant_cells: str | None = None
            breslow_thickness: float | None = None
            circumferential_resection_margin: float | None = None
            columnar_mucosa_present: str | None = None
            consistent_pathology_review: str | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            dysplasia_degree: str | None = None
            dysplasia_type: str | None = None
            greatest_tumor_dimension: float | None = None
            gross_tumor_weight: float | None = None
            largest_extrapelvic_peritoneal_focus: str | None = None
            lymph_node_involved_site: str | None = None
            lymph_node_involvement: str | None = None
            lymph_nodes_positive: int | None = None
            lymph_nodes_tested: int | None = None
            lymphatic_invasion_present: str | None = None
            margin_status: str | None = None
            metaplasia_present: str | None = None
            morphologic_architectural_pattern: str | None = None
            necrosis_percent: float | None = None
            necrosis_present: str | None = None
            non_nodal_regional_disease: str | None = None
            non_nodal_tumor_deposits: str | None = None
            number_proliferating_cells: int | None = None
            pathology_detail_id: str | None = None
            percent_tumor_invasion: float | None = None
            perineural_invasion_present: str | None = None
            peripancreatic_lymph_nodes_positive: str | None = None
            peripancreatic_lymph_nodes_tested: int | None = None
            prostatic_chips_positive_count: float | None = None
            prostatic_chips_total_count: float | None = None
            prostatic_involvement_percent: float | None = None
            residual_tumor: str | None = None
            rhabdoid_percent: float | None = None
            rhabdoid_present: str | None = None
            sarcomatoid_percent: float | None = None
            sarcomatoid_present: str | None = None
            size_extraocular_nodule: float | None = None
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            transglottic_extension: str | None = None
            tumor_largest_dimension_diameter: float | None = None
            tumor_thickness: float | None = None
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
            vascular_invasion_present: str | None = None
            vascular_invasion_type: str | None = None

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
            chemo_concurrent_to_radiation: str | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            days_to_treatment_end: int | None = None
            days_to_treatment_start: int | None = None
            initial_disease_status: str | None = None
            number_of_cycles: int | None = None
            reason_treatment_ended: str | None = None
            regimen_or_line_of_therapy: str | None = None
            route_of_administration: str | None = None
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            therapeutic_agents: str | None = None
            treatment_anatomic_site: str | None = None
            treatment_arm: str | None = None
            treatment_dose: int | None = None
            treatment_dose_units: str | None = None
            treatment_effect: str | None = None
            treatment_effect_indicator: str | None = None
            treatment_frequency: str | None = None
            treatment_id: str | None = "treatment-0"
            treatment_intent_type: str | None = None
            treatment_or_therapy: str | None = "yes"
            treatment_outcome: str | None = None
            treatment_type: str | None = "Radiation Therapy, NOS"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

        adrenal_hormone: str | None = None
        age_at_diagnosis: int | None = 22052
        ajcc_clinical_m: str | None = None
        ajcc_clinical_n: str | None = None
        ajcc_clinical_stage: str | None = None
        ajcc_clinical_t: str | None = None
        ajcc_pathologic_m: str | None = "M0"
        ajcc_pathologic_n: str | None = "N0"
        ajcc_pathologic_stage: str | None = "Stage II"
        ajcc_pathologic_t: str | None = "T2"
        ajcc_staging_system_edition: str | None = "6th"
        ann_arbor_b_symptoms: str | None = None
        ann_arbor_b_symptoms_described: str | None = None
        ann_arbor_clinical_stage: str | None = None
        ann_arbor_extranodal_involvement: str | None = None
        ann_arbor_pathologic_stage: str | None = None
        annotations: tuple[Annotation, ...] | None = (Annotation(),)
        best_overall_response: str | None = None
        burkitt_lymphoma_clinical_variant: str | None = None
        child_pugh_classification: str | None = None
        classification_of_tumor: str | None = "not reported"
        cog_liver_stage: str | None = None
        cog_neuroblastoma_risk_group: str | None = None
        cog_renal_stage: str | None = None
        cog_rhabdomyosarcoma_risk_group: str | None = None
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        days_to_best_overall_response: int | None = None
        days_to_diagnosis: int | None = 0
        days_to_last_follow_up: float | None = 795.0
        days_to_last_known_disease_status: float | None = None
        days_to_recurrence: float | None = None
        diagnosis_id: str | None = "diagnosis-0"
        eln_risk_classification: str | None = None
        enneking_msts_grade: str | None = None
        enneking_msts_metastasis: str | None = None
        enneking_msts_stage: str | None = None
        enneking_msts_tumor_site: str | None = None
        esophageal_columnar_dysplasia_degree: str | None = None
        esophageal_columnar_metaplasia_present: str | None = None
        figo_stage: str | None = None
        figo_staging_edition_year: str | None = None
        first_symptom_prior_to_diagnosis: str | None = None
        gastric_esophageal_junction_involvement: str | None = None
        gleason_grade_group: str | None = None
        gleason_grade_tertiary: str | None = None
        gleason_patterns_percent: int | None = None
        goblet_cells_columnar_mucosa_present: str | None = None
        icd_10_code: str | None = "C50.9"
        igcccg_stage: str | None = None
        inpc_grade: str | None = None
        inpc_histologic_group: str | None = None
        inrg_stage: str | None = None
        inss_stage: str | None = None
        international_prognostic_index: str | None = None
        irs_group: str | None = None
        irs_stage: str | None = None
        ishak_fibrosis_score: str | None = None
        iss_stage: str | None = None
        last_known_disease_status: str | None = "not reported"
        laterality: str | None = None
        margin_distance: float | None = None
        margins_involved_site: str | None = None
        masaoka_stage: str | None = None
        medulloblastoma_molecular_classification: str | None = None
        metastasis_at_diagnosis: str | None = None
        metastasis_at_diagnosis_site: str | None = None
        method_of_diagnosis: str | None = None
        micropapillary_features: str | None = None
        mitosis_karyorrhexis_index: str | None = None
        mitotic_count: int | None = None
        morphology: str | None = "8500/3"
        ovarian_specimen_status: str | None = None
        ovarian_surface_involvement: str | None = None
        papillary_renal_cell_type: str | None = None
        pathology_details: tuple[PathologyDetail, ...] | None = (PathologyDetail(),)
        peritoneal_fluid_cytological_status: str | None = None
        pregnant_at_diagnosis: str | None = None
        primary_diagnosis: str | None = "Infiltrating duct carcinoma, NOS"
        primary_disease: str | None = None
        primary_gleason_grade: str | None = None
        prior_malignancy: str | None = "no"
        prior_treatment: str | None = "No"
        progression_or_recurrence: str | None = "not reported"
        residual_disease: str | None = None
        satellite_nodule_present: str | None = None
        secondary_gleason_grade: str | None = None
        site_of_resection_or_biopsy: str | None = "Breast, NOS"
        sites_of_involvement: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        supratentorial_localization: str | None = None
        synchronous_malignancy: str | None = "No"
        tissue_or_organ_of_origin: str | None = "Breast, NOS"
        treatments: tuple[Treatment, ...] | None = (Treatment(),)
        tumor_confined_to_organ_of_origin: str | None = None
        tumor_depth: float | None = None
        tumor_focality: str | None = None
        tumor_grade: str | None = "not reported"
        tumor_regression_grade: str | None = None
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
        weiss_assessment_score: str | None = None
        who_cns_grade: str | None = None
        who_nte_grade: str | None = None
        wilms_tumor_histologic_subtype: str | None = None
        year_of_diagnosis: int | None = 2011

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
        age_at_onset: int | None = None
        alcohol_days_per_week: float | None = None
        alcohol_drinks_per_day: float | None = None
        alcohol_history: str | None = "Not Reported"
        alcohol_intensity: str | None = None
        alcohol_type: str | None = None
        asbestos_exposure: str | None = None
        cigarettes_per_day: float | None = None
        coal_dust_exposure: str | None = None
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        environmental_tobacco_smoke_exposure: str | None = None
        exposure_duration: str | None = None
        exposure_duration_years: int | None = None
        exposure_id: str | None = "exposure-0"
        exposure_type: str | None = None
        marijuana_use_per_week: float | None = None
        pack_years_smoked: float | None = None
        parent_with_radiation_exposure: str | None = None
        radon_exposure: str | None = None
        respirable_crystalline_silica_exposure: str | None = None
        secondhand_smoke_as_child: str | None = None
        smokeless_tobacco_quit_age: int | None = None
        smoking_frequency: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        time_between_waking_and_first_smoke: str | None = None
        tobacco_smoking_onset_year: int | None = None
        tobacco_smoking_quit_year: int | None = None
        tobacco_smoking_status: str | None = None
        tobacco_use_per_day: float | None = None
        type_of_smoke_exposure: str | None = None
        type_of_tobacco_used: str | None = None
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
        years_smoked: float | None = None

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
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        family_history_id: str | None = None
        relationship_age_at_diagnosis: float | None = None
        relationship_gender: str | None = None
        relationship_primary_diagnosis: str | None = None
        relationship_type: str | None = None
        relative_with_cancer_history: str | None = None
        relatives_with_cancer_history_count: int | None = None
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
                access: str | None = "open"
                average_base_quality: float | None = 30.0
                average_insert_size: int | None = 205
                average_read_length: int | None = 100
                channel: str | None = "Green"
                chip_id: str | None = None
                chip_position: str | None = None
                contamination: float | None = 0.002546897560409226
                contamination_error: float | None = 0.00043678932811308974
                created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                data_category: str | None = "Biospecimen"
                data_format: str | None = "BCR Biotab"
                data_type: str | None = "Biospecimen Supplement"
                error_type: str | None = "file_size"
                experimental_strategy: str | None = "WXS"
                file_id: str | None = "file-0"
                file_name: None | (
                    str
                ) = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
                file_size: int | None = 353327
                imaging_date: str | None = None
                magnification: float | None = None
                md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                mean_coverage: float | None = 96.018325
                msi_score: float | None = 0.010962821735
                msi_status: str | None = "MSS"
                pairs_on_diff_chr: int | None = 950137
                plate_name: str | None = None
                plate_well: str | None = None
                platform: str | None = "Illumina Human Methylation 450"
                proc_internal: str | None = None
                proportion_base_mismatch: float | None = 0.006583998
                proportion_coverage_10x: float | None = 0.901681
                proportion_coverage_30x: float | None = 0.785325
                proportion_reads_duplicated: float | None = 0.07279441150328658
                proportion_reads_mapped: float | None = 0.9995056930268698
                proportion_targets_no_coverage: float | None = 0.015333
                read_pair_number: str | None = None
                revision: float | None = None
                stain_type: str | None = None
                state: str | None = "released"
                state_comment: str | None = None
                submitter_id: str | None = "sub-generic-0"
                total_reads: int | None = 154707508
                tumor_ploidy: float | None = None
                tumor_purity: float | None = None
                updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
                        adapter_content: str | None = "PASS"
                        basic_statistics: str | None = "PASS"
                        created_datetime: None | (
                            str
                        ) = "2018-05-21T16:07:40.645885-05:00"
                        encoding: str | None = "Sanger / Illumina 1.9"
                        fastq_name: str | None = "122988_s.fq"
                        kmer_content: str | None = "FAIL"
                        overrepresented_sequences: str | None = "FAIL"
                        per_base_n_content: str | None = "PASS"
                        per_base_sequence_content: str | None = "FAIL"
                        per_base_sequence_quality: str | None = "FAIL"
                        per_sequence_gc_content: str | None = "FAIL"
                        per_sequence_quality_score: str | None = "PASS"
                        per_tile_sequence_quality: str | None = "PASS"
                        percent_gc_content: int | None = 46
                        read_group_qc_id: str | None = "read-group-qc-0"
                        sequence_duplication_levels: str | None = "FAIL"
                        sequence_length_distribution: str | None = "WARN"
                        state: str | None = "released"
                        submitter_id: str | None = "sub-generic-0"
                        total_sequences: int | None = 4145948
                        updated_datetime: None | (
                            str
                        ) = "2018-11-01T15:06:10.843096-05:00"
                        workflow_end_datetime: str | None = None
                        workflow_link: None | (
                            str
                        ) = "https://github.com/NCI-GDC/somatic-maf-cwl"
                        workflow_start_datetime: str | None = None
                        workflow_type: None | (
                            str
                        ) = "MuTect2 Variant Aggregation and Masking"
                        workflow_version: str | None = "v1"

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

                    adapter_name: str | None = "Ad2.19+Ad1.16"
                    adapter_sequence: str | None = None
                    base_caller_name: str | None = None
                    base_caller_version: str | None = None
                    chipseq_antibody: str | None = None
                    chipseq_target: str | None = None
                    created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                    days_to_sequencing: int | None = None
                    experiment_name: str | None = "TCGA-BH-A202-01A-11R-A14L-13"
                    flow_cell_barcode: str | None = "HKF77BBXX"
                    fragment_maximum_length: int | None = None
                    fragment_mean_length: float | None = None
                    fragment_minimum_length: int | None = None
                    fragment_standard_deviation_length: float | None = None
                    fragmentation_enzyme: str | None = None
                    includes_spike_ins: str | None = None
                    instrument_model: str | None = "Illumina HiSeq 4000"
                    is_paired_end: str | None = "True"
                    lane_number: int | None = 8
                    library_name: str | None = "MX0440"
                    library_preparation_kit_catalog_number: str | None = None
                    library_preparation_kit_name: str | None = None
                    library_preparation_kit_vendor: str | None = None
                    library_preparation_kit_version: str | None = None
                    library_selection: str | None = "miRNA Size Fractionation"
                    library_strand: str | None = None
                    library_strategy: str | None = "miRNA-Seq"
                    multiplex_barcode: str | None = "TGGTCACA+TTGATGGA"
                    number_expect_cells: int | None = None
                    platform: str | None = "Illumina Human Methylation 450"
                    read_group_id: str | None = "read-group-id"
                    read_group_name: str | None = "122988"
                    read_group_qcs: tuple[ReadGroupQc, ...] | None = (ReadGroupQc(),)
                    read_length: int | None = 15
                    rin: float | None = None
                    sequencing_center: str | None = "BCGSC"
                    sequencing_date: str | None = "2011-09-22T19"
                    single_cell_library: str | None = None
                    size_selection_range: str | None = None
                    spike_ins_concentration: str | None = None
                    spike_ins_fasta: str | None = None
                    state: str | None = "released"
                    submitter_id: str | None = "sub-generic-0"
                    target_capture_kit: str | None = "Not Applicable"
                    target_capture_kit_catalog_number: str | None = "NA"
                    target_capture_kit_name: None | (
                        str
                    ) = "hg18 nimblegen exome version 2"
                    target_capture_kit_target_region: None | (
                        str
                    ) = "ftp://genome.wustl.edu/pub/custom_capture/hg18_nimblegen_exome_version_2/hg18_nimblegen_exome_version_2.bed"
                    target_capture_kit_vendor: str | None = "Nimblegen"
                    target_capture_kit_version: str | None = None
                    to_trim_adapter_sequence: str | None = "True"
                    updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

                read_groups: tuple[ReadGroup, ...] | None = (ReadGroup(),)

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert all(
                        e.assert_equals(r)
                        for r, e in zip(row.read_groups or (), self.read_groups or ())
                    )

                    return True

            analysis_id: str | None = "analysis-0"
            analysis_type: str | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            input_files: tuple[InputFile, ...] | None = (InputFile(),)
            metadata: Metadatum | None = Metadatum()
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
            workflow_end_datetime: str | None = None
            workflow_link: str | None = "https://github.com/NCI-GDC/somatic-maf-cwl"
            workflow_start_datetime: str | None = None
            workflow_type: str | None = "MuTect2 Variant Aggregation and Masking"
            workflow_version: str | None = "v1"

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
            archive_id: str | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            data_category: str | None = "Biospecimen"
            data_format: str | None = "BCR Biotab"
            data_type: str | None = "Biospecimen Supplement"
            error_type: str | None = "file_size"
            file_name: None | (
                str
            ) = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: int | None = 353327
            md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            revision: float | None = None
            state: str | None = "released"
            state_comment: str | None = None
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
            center_id: str | None = "center-0"
            center_type: str | None = "CGCC"
            code: str | None = "20"
            name: str | None = "MD Anderson - RPPA Core Facility (Proteomics)"
            namespace: str | None = "mdanderson.org"
            short_name: str | None = "MDA"

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
                access: str | None = "open"
                average_base_quality: float | None = 30.0
                average_insert_size: int | None = 205
                average_read_length: int | None = 100
                channel: str | None = "Green"
                chip_id: str | None = None
                chip_position: str | None = None
                contamination: float | None = 0.002546897560409226
                contamination_error: float | None = 0.00043678932811308974
                created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                data_category: str | None = "Biospecimen"
                data_format: str | None = "BCR Biotab"
                data_type: str | None = "Biospecimen Supplement"
                error_type: str | None = "file_size"
                experimental_strategy: str | None = "WXS"
                file_id: str | None = "file-0"
                file_name: None | (
                    str
                ) = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
                file_size: int | None = 353327
                imaging_date: str | None = None
                magnification: float | None = None
                md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                mean_coverage: float | None = 96.018325
                msi_score: float | None = 0.010962821735
                msi_status: str | None = "MSS"
                pairs_on_diff_chr: int | None = 950137
                plate_name: str | None = None
                plate_well: str | None = None
                platform: str | None = "Illumina Human Methylation 450"
                proc_internal: str | None = None
                proportion_base_mismatch: float | None = 0.006583998
                proportion_coverage_10x: float | None = 0.901681
                proportion_coverage_30x: float | None = 0.785325
                proportion_reads_duplicated: float | None = 0.07279441150328658
                proportion_reads_mapped: float | None = 0.9995056930268698
                proportion_targets_no_coverage: float | None = 0.015333
                read_pair_number: str | None = None
                revision: float | None = None
                stain_type: str | None = None
                state: str | None = "released"
                state_comment: str | None = None
                submitter_id: str | None = "sub-generic-0"
                total_reads: int | None = 154707508
                tumor_ploidy: float | None = None
                tumor_purity: float | None = None
                updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

            analysis_id: str | None = "analysis-0"
            analysis_type: str | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            output_files: tuple[OutputFile, ...] | None = (OutputFile(),)
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
            workflow_end_datetime: str | None = None
            workflow_link: str | None = "https://github.com/NCI-GDC/somatic-maf-cwl"
            workflow_start_datetime: str | None = None
            workflow_type: str | None = "MuTect2 Variant Aggregation and Masking"
            workflow_version: str | None = "v1"

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
            access: str | None = "open"
            average_base_quality: float | None = 30.0
            average_insert_size: int | None = 205
            average_read_length: int | None = 100
            channel: str | None = "Green"
            chip_id: str | None = None
            chip_position: str | None = None
            contamination: float | None = 0.002546897560409226
            contamination_error: float | None = 0.00043678932811308974
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            data_category: str | None = "Biospecimen"
            data_format: str | None = "BCR Biotab"
            data_type: str | None = "Biospecimen Supplement"
            error_type: str | None = "file_size"
            experimental_strategy: str | None = "WXS"
            file_id: str | None = "file-0"
            file_name: None | (
                str
            ) = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: int | None = 353327
            imaging_date: str | None = None
            magnification: float | None = None
            md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            mean_coverage: float | None = 96.018325
            msi_score: float | None = 0.010962821735
            msi_status: str | None = "MSS"
            pairs_on_diff_chr: int | None = 950137
            plate_name: str | None = None
            plate_well: str | None = None
            platform: str | None = "Illumina Human Methylation 450"
            proc_internal: str | None = None
            proportion_base_mismatch: float | None = 0.006583998
            proportion_coverage_10x: float | None = 0.901681
            proportion_coverage_30x: float | None = 0.785325
            proportion_reads_duplicated: float | None = 0.07279441150328658
            proportion_reads_mapped: float | None = 0.9995056930268698
            proportion_targets_no_coverage: float | None = 0.015333
            read_pair_number: str | None = None
            revision: float | None = None
            stain_type: str | None = None
            state: str | None = "released"
            state_comment: str | None = None
            submitter_id: str | None = "sub-generic-0"
            total_reads: int | None = 154707508
            tumor_ploidy: float | None = None
            tumor_purity: float | None = None
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
            access: str | None = "open"
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            data_category: str | None = "Biospecimen"
            data_format: str | None = "BCR Biotab"
            data_type: str | None = "Biospecimen Supplement"
            error_type: str | None = "file_size"
            file_id: str | None = "file-0"
            file_name: None | (
                str
            ) = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
            file_size: int | None = 353327
            md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            state: str | None = "released"
            state_comment: str | None = None
            submitter_id: str | None = "sub-generic-0"
            type: str | None = "biospecimen_supplement"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

        access: str | None = "open"
        acl: tuple[str, ...] | None = ("open",)
        analysis: Analysi | None = Analysi()
        archive: Archive | None = Archive()
        average_base_quality: float | None = 30.0
        average_insert_size: int | None = 205
        average_read_length: int | None = 100
        center: Center | None = Center()
        channel: str | None = "Green"
        chip_id: str | None = None
        chip_position: str | None = None
        contamination: float | None = 0.002546897560409226
        contamination_error: float | None = 0.00043678932811308974
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        data_category: str | None = "Biospecimen"
        data_format: str | None = "BCR Biotab"
        data_type: str | None = "Biospecimen Supplement"
        downstream_analyses: tuple[DownstreamAnalysis, ...] | None = (
            DownstreamAnalysis(),
        )
        error_type: str | None = "file_size"
        experimental_strategy: str | None = "WXS"
        file_id: str | None = "file-0"
        file_name: str | None = "nationwidechildrens.org_ssf_tumor_samples_brca.txt"
        file_size: int | None = 353327
        imaging_date: str | None = None
        index_files: tuple[IndexFile, ...] | None = (IndexFile(),)
        magnification: float | None = None
        md5sum: str | None = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        mean_coverage: float | None = 96.018325
        metadata_files: tuple[MetadataFile, ...] | None = (MetadataFile(),)
        msi_score: float | None = 0.010962821735
        msi_status: str | None = "MSS"
        pairs_on_diff_chr: int | None = 950137
        plate_name: str | None = None
        plate_well: str | None = None
        platform: str | None = "Illumina Human Methylation 450"
        proc_internal: str | None = None
        proportion_base_mismatch: float | None = 0.006583998
        proportion_coverage_10x: float | None = 0.901681
        proportion_coverage_30x: float | None = 0.785325
        proportion_reads_duplicated: float | None = 0.07279441150328658
        proportion_reads_mapped: float | None = 0.9995056930268698
        proportion_targets_no_coverage: float | None = 0.015333
        read_pair_number: str | None = None
        revision: float | None = None
        stain_type: str | None = None
        state: str | None = "released"
        state_comment: str | None = None
        submitter_id: str | None = "sub-generic-0"
        tags: str | None = None
        total_reads: int | None = 154707508
        tumor_ploidy: float | None = None
        tumor_purity: float | None = None
        type: str | None = "biospecimen_supplement"
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
            aa_change: str | None = None
            antigen: str | None = None
            biospecimen_type: str | None = None
            biospecimen_volume: float | None = None
            blood_test_normal_range_lower: float | None = None
            blood_test_normal_range_upper: float | None = None
            cell_count: int | None = None
            chromosome: str | None = None
            clonality: str | None = None
            copy_number: float | None = None
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            cytoband: str | None = None
            days_to_test: int | None = None
            exon: str | None = None
            gene_symbol: str | None = None
            histone_family: str | None = None
            histone_variant: str | None = None
            intron: str | None = None
            laboratory_test: str | None = None
            loci_abnormal_count: int | None = None
            loci_count: int | None = None
            locus: str | None = None
            mismatch_repair_mutation: str | None = None
            mitotic_count: float | None = None
            mitotic_total_area: float | None = None
            molecular_analysis_method: str | None = None
            molecular_consequence: str | None = None
            molecular_test_id: str | None = None
            pathogenicity: str | None = None
            ploidy: str | None = None
            second_exon: str | None = None
            second_gene_symbol: str | None = None
            specialized_molecular_test: str | None = None
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            test_analyte_type: str | None = None
            test_result: str | None = None
            test_units: str | None = None
            test_value: float | None = None
            transcript: str | None = None
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
            variant_origin: str | None = None
            variant_type: str | None = None
            zygosity: str | None = None

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

        adverse_event: str | None = None
        adverse_event_grade: str | None = None
        aids_risk_factors: str | None = None
        barretts_esophagus_goblet_cells_present: str | None = None
        bmi: float | None = None
        body_surface_area: float | None = None
        cause_of_response: str | None = None
        cd4_count: float | None = None
        cdc_hiv_risk_factors: str | None = None
        comorbidity: str | None = None
        comorbidity_method_of_diagnosis: str | None = None
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        days_to_adverse_event: int | None = None
        days_to_comorbidity: int | None = None
        days_to_follow_up: int | None = None
        days_to_imaging: int | None = None
        days_to_progression: int | None = None
        days_to_progression_free: int | None = None
        days_to_recurrence: int | None = None
        diabetes_treatment_type: str | None = None
        disease_response: str | None = None
        dlco_ref_predictive_percent: float | None = None
        ecog_performance_status: str | None = None
        evidence_of_recurrence_type: str | None = None
        eye_color: str | None = None
        fev1_fvc_post_bronch_percent: float | None = None
        fev1_fvc_pre_bronch_percent: float | None = None
        fev1_ref_post_bronch_percent: float | None = None
        fev1_ref_pre_bronch_percent: float | None = None
        follow_up_id: str | None = None
        haart_treatment_indicator: str | None = None
        height: float | None = None
        hepatitis_sustained_virological_response: str | None = None
        history_of_tumor: str | None = None
        history_of_tumor_type: str | None = None
        hiv_viral_load: float | None = None
        hormonal_contraceptive_type: str | None = None
        hormonal_contraceptive_use: str | None = None
        hormone_replacement_therapy_type: str | None = None
        hpv_positive_type: str | None = None
        hysterectomy_margins_involved: str | None = None
        hysterectomy_type: str | None = None
        imaging_result: str | None = None
        imaging_type: str | None = None
        immunosuppressive_treatment_type: str | None = None
        karnofsky_performance_status: str | None = None
        menopause_status: str | None = None
        molecular_tests: tuple[MolecularTest, ...] | None = (MolecularTest(),)
        nadir_cd4_count: float | None = None
        pancreatitis_onset_year: int | None = None
        pregnancy_outcome: str | None = None
        procedures_performed: str | None = None
        progression_or_recurrence: str | None = "not reported"
        progression_or_recurrence_anatomic_site: str | None = None
        progression_or_recurrence_type: str | None = None
        recist_targeted_regions_number: int | None = None
        recist_targeted_regions_sum: float | None = None
        reflux_treatment_type: str | None = None
        risk_factor: str | None = None
        risk_factor_treatment: str | None = None
        scan_tracer_used: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        undescended_testis_corrected: str | None = None
        undescended_testis_corrected_age: int | None = None
        undescended_testis_corrected_laterality: str | None = None
        undescended_testis_corrected_method: str | None = None
        undescended_testis_history: str | None = None
        undescended_testis_history_laterality: str | None = None
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
        viral_hepatitis_serologies: str | None = None
        weight: float | None = 20.0

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
            dbgap_accession_number: str | None = "phs000178"
            name: str | None = "MD Anderson - RPPA Core Facility (Proteomics)"
            program_id: str | None = "program-0"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.dbgap_accession_number == self.dbgap_accession_number
                assert row.name == self.name
                assert row.program_id == self.program_id

                return True

        dbgap_accession_number: str | None = "phs000178"
        disease_type: tuple[str, ...] | None = ("Ductal and Lobular Neoplasms",)
        intended_release_date: str | None = None
        name: str | None = "MD Anderson - RPPA Core Facility (Proteomics)"
        primary_site: tuple[str, ...] | None = ("Breast",)
        program: Program | None = Program()
        project_id: str | None = "GDC-TEST"
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
        @dataclasses.dataclass(frozen=True)
        class Annotation:
            annotation_id: str | None = "annotation-0"
            case_id: str | None = "case-0"
            case_submitter_id: str | None = None
            category: str | None = "General"
            classification: str | None = "Observation"
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            creator: str | None = None
            entity_id: str | None = "entity-0"
            entity_submitter_id: str | None = "sub-entity-0"
            entity_type: str | None = "aliquot"
            legacy_created_datetime: str | None = None
            legacy_updated_datetime: str | None = None
            notes: None | (
                str
            ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
            state: str | None = "released"
            status: str | None = "Approved"
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
                        annotation_id: str | None = "annotation-0"
                        case_id: str | None = "case-0"
                        case_submitter_id: str | None = None
                        category: str | None = "General"
                        classification: str | None = "Observation"
                        created_datetime: None | (
                            str
                        ) = "2018-05-21T16:07:40.645885-05:00"
                        creator: str | None = None
                        entity_id: str | None = "entity-0"
                        entity_submitter_id: str | None = "sub-entity-0"
                        entity_type: str | None = "aliquot"
                        legacy_created_datetime: str | None = None
                        legacy_updated_datetime: str | None = None
                        notes: None | (
                            str
                        ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                        state: str | None = "released"
                        status: str | None = "Approved"
                        submitter_id: str | None = "sub-generic-0"
                        updated_datetime: None | (
                            str
                        ) = "2018-11-01T15:06:10.843096-05:00"

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
                        center_id: str | None = "center-0"
                        center_type: str | None = "CGCC"
                        code: str | None = "20"
                        name: None | (
                            str
                        ) = "MD Anderson - RPPA Core Facility (Proteomics)"
                        namespace: str | None = "mdanderson.org"
                        short_name: str | None = "MDA"

                        def assert_equals(self, row: sql.Row) -> bool:
                            assert row
                            assert row.center_id == self.center_id
                            assert row.center_type == self.center_type
                            assert row.code == self.code
                            assert row.name == self.name
                            assert row.namespace == self.namespace
                            assert row.short_name == self.short_name

                            return True

                    aliquot_id: str | None = "aliquot-0"
                    aliquot_quantity: float | None = 4.54
                    aliquot_volume: float | None = 26.7
                    amount: float | None = None
                    analyte_type: str | None = "RNA"
                    analyte_type_id: str | None = "R"
                    annotations: tuple[Annotation, ...] | None = (Annotation(),)
                    center: Center | None = Center()
                    concentration: float | None = 0.17
                    created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                    no_matched_normal_low_pass_wgs: str | None = None
                    no_matched_normal_targeted_sequencing: str | None = None
                    no_matched_normal_wgs: str | None = None
                    no_matched_normal_wxs: str | None = None
                    selected_normal_low_pass_wgs: str | None = None
                    selected_normal_targeted_sequencing: str | None = None
                    selected_normal_wgs: str | None = "True"
                    selected_normal_wxs: str | None = "True"
                    source_center: str | None = "23"
                    state: str | None = "released"
                    submitter_id: str | None = "sub-generic-0"
                    updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
                    annotation_id: str | None = "annotation-0"
                    case_id: str | None = "case-0"
                    case_submitter_id: str | None = None
                    category: str | None = "General"
                    classification: str | None = "Observation"
                    created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                    creator: str | None = None
                    entity_id: str | None = "entity-0"
                    entity_submitter_id: str | None = "sub-entity-0"
                    entity_type: str | None = "aliquot"
                    legacy_created_datetime: str | None = None
                    legacy_updated_datetime: str | None = None
                    notes: None | (
                        str
                    ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                    state: str | None = "released"
                    status: str | None = "Approved"
                    submitter_id: str | None = "sub-generic-0"
                    updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

                a260_a280_ratio: float | None = 1.78
                aliquots: tuple[Aliquot, ...] | None = (Aliquot(),)
                amount: float | None = None
                analyte_id: str | None = "analyte-0"
                analyte_quantity: float | None = None
                analyte_type: str | None = "RNA"
                analyte_type_id: str | None = "R"
                analyte_volume: float | None = None
                annotations: tuple[Annotation, ...] | None = (Annotation(),)
                concentration: float | None = 0.17
                created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                experimental_protocol_type: str | None = "mirVana (Allprep DNA) RNA"
                normal_tumor_genotype_snp_match: str | None = "Yes"
                ribosomal_rna_28s_16s_ratio: float | None = 1.7
                rna_integrity_number: float | None = 8.1
                spectrophotometer_method: str | None = "UV Spec"
                state: str | None = "released"
                submitter_id: str | None = "sub-generic-0"
                updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
                well_number: str | None = None

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
                annotation_id: str | None = "annotation-0"
                case_id: str | None = "case-0"
                case_submitter_id: str | None = None
                category: str | None = "General"
                classification: str | None = "Observation"
                created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                creator: str | None = None
                entity_id: str | None = "entity-0"
                entity_submitter_id: str | None = "sub-entity-0"
                entity_type: str | None = "aliquot"
                legacy_created_datetime: str | None = None
                legacy_updated_datetime: str | None = None
                notes: None | (
                    str
                ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                state: str | None = "released"
                status: str | None = "Approved"
                submitter_id: str | None = "sub-generic-0"
                updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
                center_id: str | None = "center-0"
                center_type: str | None = "CGCC"
                code: str | None = "20"
                name: str | None = "MD Anderson - RPPA Core Facility (Proteomics)"
                namespace: str | None = "mdanderson.org"
                short_name: str | None = "MDA"

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
                    annotation_id: str | None = "annotation-0"
                    case_id: str | None = "case-0"
                    case_submitter_id: str | None = None
                    category: str | None = "General"
                    classification: str | None = "Observation"
                    created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                    creator: str | None = None
                    entity_id: str | None = "entity-0"
                    entity_submitter_id: str | None = "sub-entity-0"
                    entity_type: str | None = "aliquot"
                    legacy_created_datetime: str | None = None
                    legacy_updated_datetime: str | None = None
                    notes: None | (
                        str
                    ) = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
                    state: str | None = "released"
                    status: str | None = "Approved"
                    submitter_id: str | None = "sub-generic-0"
                    updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

                annotations: tuple[Annotation, ...] | None = (Annotation(),)
                bone_marrow_malignant_cells: str | None = None
                created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
                number_proliferating_cells: int | None = None
                percent_eosinophil_infiltration: float | None = None
                percent_follicular_component: float | None = None
                percent_granulocyte_infiltration: float | None = None
                percent_inflam_infiltration: float | None = None
                percent_lymphocyte_infiltration: float | None = 0.0
                percent_monocyte_infiltration: float | None = 0.0
                percent_necrosis: float | None = 10.0
                percent_neutrophil_infiltration: float | None = 0.0
                percent_normal_cells: float | None = 0.0
                percent_rhabdoid_features: float | None = None
                percent_sarcomatoid_features: float | None = None
                percent_stromal_cells: float | None = 40.0
                percent_tumor_cells: float | None = 50.0
                percent_tumor_nuclei: float | None = 70.0
                prostatic_chips_positive_count: float | None = None
                prostatic_chips_total_count: float | None = None
                prostatic_involvement_percent: float | None = None
                section_location: str | None = "TOP"
                slide_id: str | None = "slide-0"
                state: str | None = "released"
                submitter_id: str | None = "sub-generic-0"
                tissue_microarray_coordinates: str | None = None
                updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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

            analytes: tuple[Analyte, ...] | None = (Analyte(),)
            annotations: tuple[Annotation, ...] | None = (Annotation(),)
            center: Center | None = Center()
            created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
            creation_datetime: float | None = 1311120000.0
            is_ffpe: str | None = None
            portion_id: str | None = "portion-0"
            portion_number: str | None = "21"
            slides: tuple[Slide, ...] | None = (Slide(),)
            state: str | None = "released"
            submitter_id: str | None = "sub-generic-0"
            updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"
            weight: float | None = 20.0

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

        annotations: tuple[Annotation, ...] | None = (Annotation(),)
        biospecimen_anatomic_site: str | None = None
        biospecimen_laterality: str | None = None
        catalog_reference: str | None = None
        composition: str | None = "Not Reported"
        created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
        current_weight: float | None = None
        days_to_collection: int | None = 62
        days_to_sample_procurement: int | None = 0
        diagnosis_pathologically_confirmed: str | None = None
        distance_normal_to_tumor: str | None = None
        distributor_reference: str | None = None
        freezing_method: str | None = None
        growth_rate: int | None = None
        initial_weight: float | None = 200.0
        intermediate_dimension: float | None = None
        is_ffpe: str | None = None
        longest_dimension: float | None = None
        method_of_sample_procurement: str | None = None
        oct_embedded: str | None = "true"
        passage_count: int | None = None
        pathology_report_uuid: str | None = "57323AE5-3EFE-4492-8522-D9A6DB3F1BE0"
        portions: tuple[Portion, ...] | None = (Portion(),)
        preservation_method: str | None = "FFPE"
        sample_id: str | None = "sample-0"
        sample_ordinal: int | None = None
        sample_type: str | None = "Primary Tumor"
        sample_type_id: str | None = "01"
        shortest_dimension: float | None = None
        specimen_type: str | None = "Unknown"
        state: str | None = "released"
        submitter_id: str | None = "sub-generic-0"
        time_between_clamping_and_freezing: float | None = None
        time_between_excision_and_freezing: float | None = None
        tissue_collection_type: str | None = None
        tissue_type: str | None = "Not Reported"
        tumor_code: str | None = None
        tumor_code_id: str | None = None
        tumor_descriptor: str | None = "Not Reported"
        updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
            data_category: str | None = "Biospecimen"
            file_count: int | None = 1

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.data_category == self.data_category
                assert row.file_count == self.file_count

                return True

        @dataclasses.dataclass(frozen=True)
        class ExperimentalStrategy:
            experimental_strategy: str | None = "WXS"
            file_count: int | None = 1

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.experimental_strategy == self.experimental_strategy
                assert row.file_count == self.file_count

                return True

        data_categories: tuple[DataCategory, ...] | None = (DataCategory(),)
        experimental_strategies: tuple[ExperimentalStrategy, ...] | None = (
            ExperimentalStrategy(),
        )
        file_count: int | None = 1
        file_size: int | None = 353327

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
        bcr_id: str | None = "NCH"
        code: str | None = "20"
        name: str | None = "MD Anderson - RPPA Core Facility (Proteomics)"
        project: str | None = "Breast invasive carcinoma"
        tissue_source_site_id: str | None = "tissue-source-site-0"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.bcr_id == self.bcr_id
            assert row.code == self.code
            assert row.name == self.name
            assert row.project == self.project
            assert row.tissue_source_site_id == self.tissue_source_site_id

            return True

    aliquot_ids: tuple[str, ...] | None = ("aliquot-0",)
    analyte_ids: tuple[str, ...] | None = ("analyte-0",)
    annotations: tuple[Annotation, ...] | None = (Annotation(),)
    case_id: str | None = "case-0"
    consent_type: str | None = None
    created_datetime: str | None = "2018-05-21T16:07:40.645885-05:00"
    days_to_consent: int | None = None
    days_to_lost_to_followup: int | None = None
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    diagnosis_ids: tuple[str, ...] | None = ("diagnosis-0",)
    disease_type: str | None = "Ductal and Lobular Neoplasms"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    files: tuple[File, ...] | None = (File(),)
    follow_ups: tuple[FollowUp, ...] | None = (FollowUp(),)
    index_date: str | None = None
    lost_to_followup: str | None = None
    portion_ids: tuple[str, ...] | None = ("portion-0",)
    primary_site: str | None = "Breast"
    project: Project | None = Project()
    sample_ids: tuple[str, ...] | None = ("sample-0",)
    samples: tuple[Sample, ...] = (Sample(),)
    slide_ids: tuple[str, ...] | None = ("slide-0",)
    state: str | None = "released"
    submitter_aliquot_ids: tuple[str, ...] | None = ("sub-aliquot-0",)
    submitter_analyte_ids: tuple[str, ...] | None = ("sub-analyte-0",)
    submitter_diagnosis_ids: tuple[str, ...] | None = ("sub-diagnosis-0",)
    submitter_id: str | None = "sub-generic-0"
    submitter_portion_ids: tuple[str, ...] | None = ("sub-portion-0",)
    submitter_sample_ids: tuple[str, ...] | None = ("sub-sample-0",)
    submitter_slide_ids: tuple[str, ...] | None = ("sub-slide-0",)
    summary: Summary | None = Summary()
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    updated_datetime: str | None = "2018-11-01T15:06:10.843096-05:00"

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
