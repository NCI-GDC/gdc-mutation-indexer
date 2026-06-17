import dataclasses

from pyspark import sql


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Annotation:
        annotation_id: str | None = "annotation-0"
        case_id: str | None = "case-0"
        case_submitter_id: str | None = "TEST-UNIT-case-0"
        category: str | None = "Item is noncanonical"
        classification: str | None = "Notification"
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        creator: str | None = None
        entity_id: str | None = "entity-0"
        entity_submitter_id: str | None = "TEST-UNIT-entity-0"
        entity_type: str | None = "portion"
        legacy_created_datetime: str | None = "null"
        legacy_updated_datetime: str | None = None
        notes: str | None = "CPTAC Proteomics OV PNNL"
        state: str | None = "released"
        status: str | None = "Approved"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
        age_at_index: int | None = 85
        age_is_obfuscated: str | None = "True"
        cause_of_death: str | None = "Cancer Related"
        cause_of_death_source: str | None = "Medical Record"
        country_of_birth: str | None = "United Kingdom"
        country_of_residence_at_enrollment: str | None = "United States"
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        days_to_birth: int | None = -31232
        days_to_death: int | None = 1324
        demographic_id: str | None = "demographic-0"
        education_level: str | None = None
        ethnicity: str | None = "not hispanic or latino"
        marital_status: str | None = None
        occupation_duration_years: int | None = 23
        population_group: str | None = "Ashkenazi Jew"
        race: str | None = "white"
        sex_at_birth: str | None = "female"
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
        vital_status: str | None = "Alive"
        year_of_birth: int | None = 1948
        year_of_birth_range: str | None = None
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
            assert row.created_datetime == self.created_datetime
            assert row.days_to_birth == self.days_to_birth
            assert row.days_to_death == self.days_to_death
            assert row.demographic_id == self.demographic_id
            assert row.education_level == self.education_level
            assert row.ethnicity == self.ethnicity
            assert row.marital_status == self.marital_status
            assert row.occupation_duration_years == self.occupation_duration_years
            assert row.population_group == self.population_group
            assert row.race == self.race
            assert row.sex_at_birth == self.sex_at_birth
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.updated_datetime == self.updated_datetime
            assert row.vital_status == self.vital_status
            assert row.year_of_birth == self.year_of_birth
            assert row.year_of_birth_range == self.year_of_birth_range
            assert row.year_of_death == self.year_of_death

            return True

    @dataclasses.dataclass(frozen=True)
    class Diagnosis:
        @dataclasses.dataclass(frozen=True)
        class Annotation:
            annotation_id: str | None = "annotation-0"
            case_id: str | None = "case-0"
            case_submitter_id: str | None = "TEST-UNIT-case-0"
            category: str | None = "Item is noncanonical"
            classification: str | None = "Notification"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            creator: str | None = None
            entity_id: str | None = "entity-0"
            entity_submitter_id: str | None = "TEST-UNIT-entity-0"
            entity_type: str | None = "portion"
            legacy_created_datetime: str | None = "null"
            legacy_updated_datetime: str | None = None
            notes: str | None = "CPTAC Proteomics OV PNNL"
            state: str | None = "released"
            status: str | None = "Approved"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
            additional_pathology_findings: str | None = "Extravascular Matrix Loops"
            anaplasia_present: str | None = None
            anaplasia_present_type: str | None = "Unknown"
            bone_marrow_malignant_cells: str | None = None
            breslow_thickness: float | None = 3.0
            breslow_thickness_category: str | None = None
            circumferential_resection_margin: float | None = 6.0
            columnar_mucosa_present: str | None = None
            consistent_pathology_review: str | None = "Not Reported"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            days_to_pathology_detail: int | None = None
            dysplasia_degree: str | None = None
            dysplasia_type: str | None = None
            epithelioid_cell_percent_range: str | None = ">90%"
            extracapsular_extension: str | None = "Extensive"
            extracapsular_extension_present: str | None = "True"
            extranodal_extension: str | None = "Gross Extension"
            extraocular_nodule_size: str | None = "<=5mm"
            extrascleral_extension_present: str | None = None
            extrathyroid_extension: str | None = "None"
            greatest_tumor_dimension: float | None = 3.0
            gross_tumor_weight: float | None = 300.0
            histologic_progression_type: str | None = None
            intratubular_germ_cell_neoplasia_present: str | None = "True"
            largest_extrapelvic_peritoneal_focus: str | None = "Macroscopic (greater than 2cm)"
            lymph_node_dissection_method: str | None = "Functional (Limited) Neck Dissection"
            lymph_node_dissection_site: str | None = "Retroperitoneal"
            lymph_node_involved_site: str | None = "Pelvis, NOS"
            lymph_node_involvement: str | None = "Positive"
            lymph_nodes_positive: int | None = 1
            lymph_nodes_removed: str | None = None
            lymph_nodes_tested: int | None = 7
            lymphatic_invasion_present: str | None = "True"
            margin_status: str | None = "Uninvolved"
            measurement_type: str | None = "Pathologic"
            measurement_unit: str | None = "Centimeters"
            metaplasia_present: str | None = None
            micrometastasis_present: str | None = None
            morphologic_architectural_pattern: str | None = "Cohesive"
            necrosis_percent: float | None = 10.0
            necrosis_present: str | None = "True"
            non_nodal_regional_disease: str | None = None
            non_nodal_tumor_deposits: str | None = "True"
            number_proliferating_cells: int | None = None
            pathology_detail_id: str | None = "pathology-detail-0"
            percent_tumor_invasion: float | None = 8.3
            percent_tumor_nuclei: float | None = 95.0
            perineural_invasion_present: str | None = None
            peripancreatic_lymph_nodes_positive: str | None = "1-3"
            peripancreatic_lymph_nodes_tested: int | None = 77
            prcc_type: str | None = "Unknown"
            prostatic_chips_positive_count: float | None = None
            prostatic_chips_total_count: float | None = None
            prostatic_involvement_percent: float | None = None
            residual_tumor: str | None = None
            residual_tumor_measurement: str | None = "1-10 mm"
            rhabdoid_percent: float | None = None
            rhabdoid_present: str | None = None
            sarcomatoid_percent: float | None = 90.0
            sarcomatoid_present: str | None = None
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
            vascular_invasion_present: str | None = "True"
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
                assert (
                    row.epithelioid_cell_percent_range == self.epithelioid_cell_percent_range
                )
                assert row.extracapsular_extension == self.extracapsular_extension
                assert (
                    row.extracapsular_extension_present == self.extracapsular_extension_present
                )
                assert row.extranodal_extension == self.extranodal_extension
                assert row.extraocular_nodule_size == self.extraocular_nodule_size
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
            chemo_concurrent_to_radiation: str | None = "True"
            clinical_trial_indicator: str | None = None
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
            radiosensitizing_agent: str | None = None
            reason_treatment_ended: str | None = "Course of Therapy Completed"
            reason_treatment_not_given: str | None = "Not Reported"
            regimen_or_line_of_therapy: str | None = "TIP"
            residual_disease: str | None = "R0"
            route_of_administration: tuple[str, ...] | None = ("Intravenous",)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            therapeutic_agents: str | None = "Gemcitabine Hydrochloride"
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
            treatment_or_therapy: str | None = None
            treatment_outcome: str | None = "Not Reported"
            treatment_outcome_duration: int | None = 366
            treatment_type: str | None = "Radiation Therapy, NOS"
            treatment_type_administered: str | None = None
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
                assert row.treatment_type_administered == self.treatment_type_administered
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
        ann_arbor_b_symptoms: str | None = "True"
        ann_arbor_b_symptoms_described: str | None = None
        ann_arbor_b_symptoms_described_array: tuple[str, ...] | None = (
            "Fever",
            "Night Sweats",
        )
        ann_arbor_clinical_stage: str | None = "Stage III"
        ann_arbor_extranodal_involvement: str | None = None
        ann_arbor_pathologic_stage: str | None = "Stage III"
        annotations: tuple[Annotation, ...] | None = (Annotation(),)
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
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        days_to_best_overall_response: int | None = 850
        days_to_diagnosis: int | None = 505
        days_to_last_follow_up: float | None = 84.0
        days_to_last_known_disease_status: float | None = 510.0
        days_to_recurrence: float | None = 505.0
        diagnosis_id: str | None = "diagnosis-0"
        diagnosis_is_primary_disease: str | None = "True"
        double_expressor_lymphoma: str | None = None
        double_hit_lymphoma: str | None = None
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
        esophageal_columnar_metaplasia_present: str | None = "True"
        fab_morphology_code: str | None = "M1"
        figo_stage: str | None = "Stage IIIC"
        figo_staging_edition_year: str | None = "2009"
        first_symptom_longest_duration: str | None = ">=181 Days"
        first_symptom_prior_to_diagnosis: str | None = "Sensory Changes"
        gastric_esophageal_junction_involvement: str | None = "True"
        gleason_grade_group: str | None = None
        gleason_grade_tertiary: str | None = "Pattern 5"
        gleason_patterns_percent: int | None = None
        gleason_score: int | None = 7
        goblet_cells_columnar_mucosa_present: str | None = "True"
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
        melanoma_known_primary: str | None = "True"
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
        prior_treatment: str | None = None
        progression_or_recurrence: str | None = "True"
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
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
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
            assert row.created_datetime == self.created_datetime
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
            assert row.updated_datetime == self.updated_datetime
            assert row.weiss_assessment_score == self.weiss_assessment_score
            assert row.who_cns_grade == self.who_cns_grade
            assert row.who_nte_grade == self.who_nte_grade
            assert row.wilms_tumor_histologic_subtype == self.wilms_tumor_histologic_subtype
            assert row.year_of_diagnosis == self.year_of_diagnosis
            assert (
                tuple(row.ann_arbor_b_symptoms_described_array)
                == self.ann_arbor_b_symptoms_described_array
            )
            assert tuple(row.sites_of_involvement) == self.sites_of_involvement
            assert tuple(row.weiss_assessment_findings) == self.weiss_assessment_findings
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.annotations or (), self.annotations or ())
            )
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
        alcohol_history: str | None = "True"
        alcohol_intensity: str | None = "Drinker"
        alcohol_type: str | None = "Liquor"
        asbestos_exposure_type: str | None = "Crocidolite"
        chemical_exposure_type: tuple[str, ...] | None = ("Chemical Exposure, NOS",)
        cigarettes_per_day: float | None = 20.0
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
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
        parent_with_radiation_exposure: str | None = "True"
        secondhand_smoke_as_child: str | None = "True"
        smoking_frequency: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        time_between_waking_and_first_smoke: str | None = None
        tobacco_smoking_onset_year: int | None = 1946
        tobacco_smoking_quit_year: int | None = 1981
        tobacco_smoking_status: str | None = "Current Smoker"
        type_of_smoke_exposure: str | None = "Smoke exposure, NOS"
        type_of_tobacco_used: str | None = "Smokeless Tobacco"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
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
            assert row.created_datetime == self.created_datetime
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
            assert row.updated_datetime == self.updated_datetime
            assert row.use_per_day == self.use_per_day
            assert tuple(row.chemical_exposure_type) == self.chemical_exposure_type
            assert tuple(row.occupation_type) == self.occupation_type

            return True

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        family_history_id: str | None = "family-history-0"
        relationship_age_at_diagnosis: float | None = None
        relationship_primary_diagnosis: str | None = "Lung Cancer"
        relationship_sex_at_birth: str | None = "female"
        relationship_type: str | None = "Sibling"
        relative_deceased: str | None = None
        relative_smoker: str | None = None
        relative_with_cancer_history: str | None = None
        relatives_with_cancer_history_count: int | None = 1
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.created_datetime == self.created_datetime
            assert row.family_history_id == self.family_history_id
            assert row.relationship_age_at_diagnosis == self.relationship_age_at_diagnosis
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
            assert row.updated_datetime == self.updated_datetime

            return True

    @dataclasses.dataclass(frozen=True)
    class File:
        @dataclasses.dataclass(frozen=True)
        class Analysi:
            @dataclasses.dataclass(frozen=True)
            class InputFile:
                access: str | None = "controlled"
                average_base_quality: float | None = 36.0
                average_insert_size: int | None = 327
                average_read_length: int | None = 29
                cancer_dna_fraction: float | None = None
                channel: str | None = "Green"
                chip_id: str | None = "chip-0"
                chip_position: str | None = "R08C01"
                contamination: float | None = 0.008897097478955094
                contamination_error: float | None = 0.00048251285734173503
                created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                data_category: str | None = "Simple Nucleotide Variation"
                data_format: str | None = "TBI"
                data_type: str | None = "Somatic Mutation Index"
                error_type: str | None = "file_size"
                experimental_strategy: str | None = "WXS"
                file_id: str | None = "file-0"
                file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
                file_size: int | None = 5777
                genome_doubling: int | None = None
                imaging_date: str | None = None
                magnification: float | None = 20.0
                md5sum: str | None = "ffffffffffffffffffffffffffffffff"
                mean_coverage: float | None = 91.211552
                msi_score: float | None = 0.0290948275862
                msi_status: str | None = "MSS"
                pairs_on_diff_chr: int | None = 16496391
                plate_name: str | None = "EPIC_250ng_042722_01"
                plate_well: str | None = "H07"
                platform: str | None = "Illumina"
                proc_internal: str | None = None
                proportion_base_mismatch: float | None = 0.004865963
                proportion_coverage_10x: float | None = 0.96331
                proportion_coverage_30x: float | None = 0.956296
                proportion_reads_duplicated: float | None = 0.03719862840124014
                proportion_reads_mapped: float | None = 0.9922124119944219
                proportion_targets_no_coverage: float | None = 0.114887
                read_pair_number: str | None = None
                revision: float | None = 57.0
                stain_type: str | None = None
                state: str | None = "released"
                state_comment: str | None = None
                subclonal_genome_fraction: float | None = None
                submitter_id: str | None = "TEST-UNIT-submitter-0"
                tmb: float | None = None
                tmb_exonic: float | None = None
                tmb_nonsynonymous: float | None = None
                tmb_nonsynonymous_exonic: float | None = None
                total_reads: int | None = 28301189
                tumor_ploidy: float | None = 1.75198424956185
                tumor_purity: float | None = 0.90969765412374
                updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
                wgs_coverage: str | None = "Not Applicable"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.access == self.access
                    assert row.average_base_quality == self.average_base_quality
                    assert row.average_insert_size == self.average_insert_size
                    assert row.average_read_length == self.average_read_length
                    assert row.cancer_dna_fraction == self.cancer_dna_fraction
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
                    assert row.genome_doubling == self.genome_doubling
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
                    assert row.subclonal_genome_fraction == self.subclonal_genome_fraction
                    assert row.submitter_id == self.submitter_id
                    assert row.tmb == self.tmb
                    assert row.tmb_exonic == self.tmb_exonic
                    assert row.tmb_nonsynonymous == self.tmb_nonsynonymous
                    assert row.tmb_nonsynonymous_exonic == self.tmb_nonsynonymous_exonic
                    assert row.total_reads == self.total_reads
                    assert row.tumor_ploidy == self.tumor_ploidy
                    assert row.tumor_purity == self.tumor_purity
                    assert row.updated_datetime == self.updated_datetime
                    assert row.wgs_coverage == self.wgs_coverage

                    return True

            @dataclasses.dataclass(frozen=True)
            class Metadatum:
                @dataclasses.dataclass(frozen=True)
                class ReadGroup:
                    @dataclasses.dataclass(frozen=True)
                    class ReadGroupQc:
                        adapter_content: str | None = "PASS"
                        basic_statistics: str | None = "PASS"
                        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                        encoding: str | None = "Sanger / Illumina 1.9"
                        fastq_name: str | None = "126378_s.fq"
                        kmer_content: str | None = "FAIL"
                        overrepresented_sequences: str | None = "PASS"
                        per_base_n_content: str | None = "PASS"
                        per_base_sequence_content: str | None = "WARN"
                        per_base_sequence_quality: str | None = "FAIL"
                        per_sequence_gc_content: str | None = "PASS"
                        per_sequence_quality_score: str | None = "PASS"
                        per_tile_sequence_quality: str | None = "PASS"
                        percent_gc_content: int | None = 49
                        read_group_qc_id: str | None = "read-group-qc-0"
                        sequence_duplication_levels: str | None = "PASS"
                        sequence_length_distribution: str | None = "PASS"
                        state: str | None = "released"
                        submitter_id: str | None = "TEST-UNIT-submitter-0"
                        total_sequences: int | None = 69805122
                        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
                        workflow_end_datetime: str | None = None
                        workflow_link: str | None = (
                            "https://github.com/NCI-GDC/workflows/magic"
                        )
                        workflow_start_datetime: str | None = None
                        workflow_type: str | None = "Pindel Annotation"
                        workflow_version: str | None = "wfv0"

                        def assert_equals(self, row: sql.Row) -> bool:
                            assert row
                            assert row.adapter_content == self.adapter_content
                            assert row.basic_statistics == self.basic_statistics
                            assert row.created_datetime == self.created_datetime
                            assert row.encoding == self.encoding
                            assert row.fastq_name == self.fastq_name
                            assert row.kmer_content == self.kmer_content
                            assert (
                                row.overrepresented_sequences == self.overrepresented_sequences
                            )
                            assert row.per_base_n_content == self.per_base_n_content
                            assert (
                                row.per_base_sequence_content == self.per_base_sequence_content
                            )
                            assert (
                                row.per_base_sequence_quality == self.per_base_sequence_quality
                            )
                            assert row.per_sequence_gc_content == self.per_sequence_gc_content
                            assert (
                                row.per_sequence_quality_score
                                == self.per_sequence_quality_score
                            )
                            assert (
                                row.per_tile_sequence_quality == self.per_tile_sequence_quality
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
                            assert row.workflow_end_datetime == self.workflow_end_datetime
                            assert row.workflow_link == self.workflow_link
                            assert row.workflow_start_datetime == self.workflow_start_datetime
                            assert row.workflow_type == self.workflow_type
                            assert row.workflow_version == self.workflow_version

                            return True

                    adapter_name: str | None = "Ad2.22+Ad1.15"
                    adapter_sequence: str | None = "GCCAAT"
                    base_caller_name: str | None = "bcl2fastq2"
                    base_caller_version: str | None = "v2.20.0.422"
                    chipseq_antibody: str | None = "Unknown"
                    chipseq_target: str | None = "Unknown"
                    created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                    days_to_sequencing: int | None = None
                    experiment_name: str | None = (
                        "240620_UNC41-A00434_0777_AHJ332DSXC_AGAGGCTC-ACTGAGAC_S232_L003"
                    )
                    flow_cell_barcode: str | None = "AHJ332DSXC"
                    fragment_maximum_length: int | None = 247531395
                    fragment_mean_length: float | None = 325.86892
                    fragment_minimum_length: int | None = 2
                    fragment_standard_deviation_length: float | None = 106.90892
                    fragmentation_enzyme: str | None = None
                    includes_spike_ins: str | None = "True"
                    instrument_model: str | None = "Illumina HiSeq 4000"
                    is_paired_end: str | None = "True"
                    lane_number: int | None = 1
                    library_name: str | None = "00C0029551"
                    library_preparation_kit_catalog_number: str | None = "KK8505"
                    library_preparation_kit_name: str | None = (
                        "NEXTFLEX Small RNA Sequencing Kit V4"
                    )
                    library_preparation_kit_vendor: str | None = "Illumina"
                    library_preparation_kit_version: str | None = "v1.1"
                    library_selection: str | None = "rRNA Depletion"
                    library_strand: str | None = "First_Stranded"
                    library_strategy: str | None = "miRNA-Seq"
                    multiplex_barcode: str | None = "AGAGGCTC+ACTGAGAC"
                    number_expect_cells: int | None = 9000
                    platform: str | None = "Illumina"
                    read_group_id: str | None = "read-group-0"
                    read_group_name: str | None = (
                        "240620_UNC41-A00434_0777_AHJ332DSXC_AGAGGCTC-ACTGAGAC_S232_L003"
                    )
                    read_group_qcs: tuple[ReadGroupQc, ...] | None = (ReadGroupQc(),)
                    read_length: int | None = 50
                    rin: float | None = None
                    sequencing_center: str | None = "unc.edu"
                    sequencing_date: str | None = "20240620"
                    single_cell_library: str | None = "Chromium 3' Gene Expression v3 Library"
                    size_selection_range: str | None = "168"
                    spike_ins_concentration: str | None = "2"
                    spike_ins_fasta: str | None = "https://www.ncbi.nlm.nih.gov/"
                    state: str | None = "released"
                    submitter_id: str | None = "TEST-UNIT-submitter-0"
                    target_capture_kit: str | None = "Not Applicable"
                    target_capture_kit_catalog_number: str | None = "Obsolete"
                    target_capture_kit_name: str | None = (
                        "NimbleGen Sequence Capture 2.1M Human Exome Array"
                    )
                    target_capture_kit_target_region: str | None = "Deleware"
                    target_capture_kit_vendor: str | None = "Nimblegen"
                    target_capture_kit_version: str | None = "Not Applicable"
                    to_trim_adapter_sequence: str | None = "True"
                    updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
                        assert row.fragment_maximum_length == self.fragment_maximum_length
                        assert row.fragment_mean_length == self.fragment_mean_length
                        assert row.fragment_minimum_length == self.fragment_minimum_length
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
                        assert row.spike_ins_concentration == self.spike_ins_concentration
                        assert row.spike_ins_fasta == self.spike_ins_fasta
                        assert row.state == self.state
                        assert row.submitter_id == self.submitter_id
                        assert row.target_capture_kit == self.target_capture_kit
                        assert (
                            row.target_capture_kit_catalog_number
                            == self.target_capture_kit_catalog_number
                        )
                        assert row.target_capture_kit_name == self.target_capture_kit_name
                        assert (
                            row.target_capture_kit_target_region
                            == self.target_capture_kit_target_region
                        )
                        assert row.target_capture_kit_vendor == self.target_capture_kit_vendor
                        assert (
                            row.target_capture_kit_version == self.target_capture_kit_version
                        )
                        assert row.to_trim_adapter_sequence == self.to_trim_adapter_sequence
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
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            input_files: tuple[InputFile, ...] | None = (InputFile(),)
            metadata: Metadatum | None = Metadatum()
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
            workflow_end_datetime: str | None = None
            workflow_link: str | None = "https://github.com/NCI-GDC/workflows/magic"
            workflow_start_datetime: str | None = None
            workflow_type: str | None = "Pindel Annotation"
            workflow_version: str | None = "wfv0"

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
            archive_id: str | None = "archive-0"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            data_category: str | None = "Simple Nucleotide Variation"
            data_format: str | None = "TBI"
            data_type: str | None = "Somatic Mutation Index"
            error_type: str | None = "file_size"
            file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
            file_size: int | None = 5777
            md5sum: str | None = "ffffffffffffffffffffffffffffffff"
            revision: float | None = 57.0
            state: str | None = "released"
            state_comment: str | None = None
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
            center_type: str | None = "GSC"
            code: str | None = "10"
            name: str | None = "Baylor College of Medicine"
            namespace: str | None = "hgsc.bcm.edu"
            short_name: str | None = "BCM"

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
                access: str | None = "controlled"
                average_base_quality: float | None = 36.0
                average_insert_size: int | None = 327
                average_read_length: int | None = 29
                cancer_dna_fraction: float | None = None
                channel: str | None = "Green"
                chip_id: str | None = "chip-0"
                chip_position: str | None = "R08C01"
                contamination: float | None = 0.008897097478955094
                contamination_error: float | None = 0.00048251285734173503
                created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                data_category: str | None = "Simple Nucleotide Variation"
                data_format: str | None = "TBI"
                data_type: str | None = "Somatic Mutation Index"
                error_type: str | None = "file_size"
                experimental_strategy: str | None = "WXS"
                file_id: str | None = "file-0"
                file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
                file_size: int | None = 5777
                genome_doubling: int | None = None
                imaging_date: str | None = None
                magnification: float | None = 20.0
                md5sum: str | None = "ffffffffffffffffffffffffffffffff"
                mean_coverage: float | None = 91.211552
                msi_score: float | None = 0.0290948275862
                msi_status: str | None = "MSS"
                pairs_on_diff_chr: int | None = 16496391
                plate_name: str | None = "EPIC_250ng_042722_01"
                plate_well: str | None = "H07"
                platform: str | None = "Illumina"
                proc_internal: str | None = None
                proportion_base_mismatch: float | None = 0.004865963
                proportion_coverage_10x: float | None = 0.96331
                proportion_coverage_30x: float | None = 0.956296
                proportion_reads_duplicated: float | None = 0.03719862840124014
                proportion_reads_mapped: float | None = 0.9922124119944219
                proportion_targets_no_coverage: float | None = 0.114887
                read_pair_number: str | None = None
                revision: float | None = 57.0
                stain_type: str | None = None
                state: str | None = "released"
                state_comment: str | None = None
                subclonal_genome_fraction: float | None = None
                submitter_id: str | None = "TEST-UNIT-submitter-0"
                tmb: float | None = None
                tmb_exonic: float | None = None
                tmb_nonsynonymous: float | None = None
                tmb_nonsynonymous_exonic: float | None = None
                total_reads: int | None = 28301189
                tumor_ploidy: float | None = 1.75198424956185
                tumor_purity: float | None = 0.90969765412374
                updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
                wgs_coverage: str | None = "Not Applicable"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.access == self.access
                    assert row.average_base_quality == self.average_base_quality
                    assert row.average_insert_size == self.average_insert_size
                    assert row.average_read_length == self.average_read_length
                    assert row.cancer_dna_fraction == self.cancer_dna_fraction
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
                    assert row.genome_doubling == self.genome_doubling
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
                    assert row.subclonal_genome_fraction == self.subclonal_genome_fraction
                    assert row.submitter_id == self.submitter_id
                    assert row.tmb == self.tmb
                    assert row.tmb_exonic == self.tmb_exonic
                    assert row.tmb_nonsynonymous == self.tmb_nonsynonymous
                    assert row.tmb_nonsynonymous_exonic == self.tmb_nonsynonymous_exonic
                    assert row.total_reads == self.total_reads
                    assert row.tumor_ploidy == self.tumor_ploidy
                    assert row.tumor_purity == self.tumor_purity
                    assert row.updated_datetime == self.updated_datetime
                    assert row.wgs_coverage == self.wgs_coverage

                    return True

            analysis_id: str | None = "analysis-0"
            analysis_type: str | None = None
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            output_files: tuple[OutputFile, ...] | None = (OutputFile(),)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
            workflow_end_datetime: str | None = None
            workflow_link: str | None = "https://github.com/NCI-GDC/workflows/magic"
            workflow_start_datetime: str | None = None
            workflow_type: str | None = "Pindel Annotation"
            workflow_version: str | None = "wfv0"

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
            access: str | None = "controlled"
            average_base_quality: float | None = 36.0
            average_insert_size: int | None = 327
            average_read_length: int | None = 29
            cancer_dna_fraction: float | None = None
            channel: str | None = "Green"
            chip_id: str | None = "chip-0"
            chip_position: str | None = "R08C01"
            contamination: float | None = 0.008897097478955094
            contamination_error: float | None = 0.00048251285734173503
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            data_category: str | None = "Simple Nucleotide Variation"
            data_format: str | None = "TBI"
            data_type: str | None = "Somatic Mutation Index"
            error_type: str | None = "file_size"
            experimental_strategy: str | None = "WXS"
            file_id: str | None = "file-0"
            file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
            file_size: int | None = 5777
            genome_doubling: int | None = None
            imaging_date: str | None = None
            magnification: float | None = 20.0
            md5sum: str | None = "ffffffffffffffffffffffffffffffff"
            mean_coverage: float | None = 91.211552
            msi_score: float | None = 0.0290948275862
            msi_status: str | None = "MSS"
            pairs_on_diff_chr: int | None = 16496391
            plate_name: str | None = "EPIC_250ng_042722_01"
            plate_well: str | None = "H07"
            platform: str | None = "Illumina"
            proc_internal: str | None = None
            proportion_base_mismatch: float | None = 0.004865963
            proportion_coverage_10x: float | None = 0.96331
            proportion_coverage_30x: float | None = 0.956296
            proportion_reads_duplicated: float | None = 0.03719862840124014
            proportion_reads_mapped: float | None = 0.9922124119944219
            proportion_targets_no_coverage: float | None = 0.114887
            read_pair_number: str | None = None
            revision: float | None = 57.0
            stain_type: str | None = None
            state: str | None = "released"
            state_comment: str | None = None
            subclonal_genome_fraction: float | None = None
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            tmb: float | None = None
            tmb_exonic: float | None = None
            tmb_nonsynonymous: float | None = None
            tmb_nonsynonymous_exonic: float | None = None
            total_reads: int | None = 28301189
            tumor_ploidy: float | None = 1.75198424956185
            tumor_purity: float | None = 0.90969765412374
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
            wgs_coverage: str | None = "Not Applicable"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.access == self.access
                assert row.average_base_quality == self.average_base_quality
                assert row.average_insert_size == self.average_insert_size
                assert row.average_read_length == self.average_read_length
                assert row.cancer_dna_fraction == self.cancer_dna_fraction
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
                assert row.genome_doubling == self.genome_doubling
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
                    row.proportion_targets_no_coverage == self.proportion_targets_no_coverage
                )
                assert row.read_pair_number == self.read_pair_number
                assert row.revision == self.revision
                assert row.stain_type == self.stain_type
                assert row.state == self.state
                assert row.state_comment == self.state_comment
                assert row.subclonal_genome_fraction == self.subclonal_genome_fraction
                assert row.submitter_id == self.submitter_id
                assert row.tmb == self.tmb
                assert row.tmb_exonic == self.tmb_exonic
                assert row.tmb_nonsynonymous == self.tmb_nonsynonymous
                assert row.tmb_nonsynonymous_exonic == self.tmb_nonsynonymous_exonic
                assert row.total_reads == self.total_reads
                assert row.tumor_ploidy == self.tumor_ploidy
                assert row.tumor_purity == self.tumor_purity
                assert row.updated_datetime == self.updated_datetime
                assert row.wgs_coverage == self.wgs_coverage

                return True

        @dataclasses.dataclass(frozen=True)
        class MetadataFile:
            access: str | None = "controlled"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            data_category: str | None = "Simple Nucleotide Variation"
            data_format: str | None = "TBI"
            data_type: str | None = "Somatic Mutation Index"
            error_type: str | None = "file_size"
            file_id: str | None = "file-0"
            file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
            file_size: int | None = 5777
            md5sum: str | None = "ffffffffffffffffffffffffffffffff"
            state: str | None = "released"
            state_comment: str | None = None
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            type: str | None = "annotated_somatic_mutation"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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

        access: str | None = "controlled"
        acl: tuple[str, ...] | None = ("phs000178",)
        analysis: Analysi | None = Analysi()
        archive: Archive | None = Archive()
        average_base_quality: float | None = 36.0
        average_insert_size: int | None = 327
        average_read_length: int | None = 29
        cancer_dna_fraction: float | None = None
        center: Center | None = Center()
        channel: str | None = "Green"
        chip_id: str | None = "chip-0"
        chip_position: str | None = "R08C01"
        contamination: float | None = 0.008897097478955094
        contamination_error: float | None = 0.00048251285734173503
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        data_category: str | None = "Simple Nucleotide Variation"
        data_format: str | None = "TBI"
        data_type: str | None = "Somatic Mutation Index"
        downstream_analyses: tuple[DownstreamAnalysis, ...] | None = (DownstreamAnalysis(),)
        error_type: str | None = "file_size"
        experimental_strategy: str | None = "WXS"
        file_id: str | None = "file-0"
        file_name: str | None = "TEST-UNIT.important_file.vcf.gz.tbi"
        file_size: int | None = 5777
        genome_doubling: int | None = None
        imaging_date: str | None = None
        index_files: tuple[IndexFile, ...] | None = (IndexFile(),)
        magnification: float | None = 20.0
        md5sum: str | None = "ffffffffffffffffffffffffffffffff"
        mean_coverage: float | None = 91.211552
        metadata_files: tuple[MetadataFile, ...] | None = (MetadataFile(),)
        msi_score: float | None = 0.0290948275862
        msi_status: str | None = "MSS"
        pairs_on_diff_chr: int | None = 16496391
        plate_name: str | None = "EPIC_250ng_042722_01"
        plate_well: str | None = "H07"
        platform: str | None = "Illumina"
        proc_internal: str | None = None
        proportion_base_mismatch: float | None = 0.004865963
        proportion_coverage_10x: float | None = 0.96331
        proportion_coverage_30x: float | None = 0.956296
        proportion_reads_duplicated: float | None = 0.03719862840124014
        proportion_reads_mapped: float | None = 0.9922124119944219
        proportion_targets_no_coverage: float | None = 0.114887
        read_pair_number: str | None = None
        revision: float | None = 57.0
        stain_type: str | None = None
        state: str | None = "released"
        state_comment: str | None = None
        subclonal_genome_fraction: float | None = None
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        tags: str | None = None
        tmb: float | None = None
        tmb_exonic: float | None = None
        tmb_nonsynonymous: float | None = None
        tmb_nonsynonymous_exonic: float | None = None
        total_reads: int | None = 28301189
        tumor_ploidy: float | None = 1.75198424956185
        tumor_purity: float | None = 0.90969765412374
        type: str | None = "annotated_somatic_mutation"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
        wgs_coverage: str | None = "Not Applicable"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.access == self.access
            assert row.average_base_quality == self.average_base_quality
            assert row.average_insert_size == self.average_insert_size
            assert row.average_read_length == self.average_read_length
            assert row.cancer_dna_fraction == self.cancer_dna_fraction
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
            assert row.genome_doubling == self.genome_doubling
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
            assert row.proportion_targets_no_coverage == self.proportion_targets_no_coverage
            assert row.read_pair_number == self.read_pair_number
            assert row.revision == self.revision
            assert row.stain_type == self.stain_type
            assert row.state == self.state
            assert row.state_comment == self.state_comment
            assert row.subclonal_genome_fraction == self.subclonal_genome_fraction
            assert row.submitter_id == self.submitter_id
            assert row.tags == self.tags
            assert row.tmb == self.tmb
            assert row.tmb_exonic == self.tmb_exonic
            assert row.tmb_nonsynonymous == self.tmb_nonsynonymous
            assert row.tmb_nonsynonymous_exonic == self.tmb_nonsynonymous_exonic
            assert row.total_reads == self.total_reads
            assert row.tumor_ploidy == self.tumor_ploidy
            assert row.tumor_purity == self.tumor_purity
            assert row.type == self.type
            assert row.updated_datetime == self.updated_datetime
            assert row.wgs_coverage == self.wgs_coverage
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
                for r, e in zip(row.downstream_analyses or (), self.downstream_analyses or ())
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
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
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
            mismatch_repair_mutation: str | None = "True"
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
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
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
                assert row.created_datetime == self.created_datetime
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
                assert row.updated_datetime == self.updated_datetime
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
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
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
            haart_treatment_indicator: str | None = None
            height: float | None = 148.0
            hepatitis_sustained_virological_response: str | None = None
            hiv_viral_load: float | None = 46414.0
            hormonal_contraceptive_type: str | None = "Unknown"
            hormonal_contraceptive_use: str | None = "Never Used"
            hormonal_replacement_therapy_status: str | None = "True"
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
            pregnant_at_diagnosis: str | None = None
            premature_at_birth: str | None = None
            reflux_treatment_type: str | None = "Medically Treated"
            risk_factor_method_of_diagnosis: str | None = (
                "Both Clinical and Biochemical Assessments"
            )
            risk_factor_treatment: str | None = "True"
            risk_factors: tuple[str, ...] | None = ("Undescended Testis",)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            timepoint_category: str | None = "Last Contact"
            treatment_frequency: str | None = "Once Weekly"
            undescended_testis_corrected: str | None = "True"
            undescended_testis_corrected_age_range: str | None = "2-11 months"
            undescended_testis_corrected_laterality: str | None = "Right"
            undescended_testis_corrected_method: str | None = "Orchiopexy"
            undescended_testis_history: str | None = "True"
            undescended_testis_history_laterality: str | None = "Right"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
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
                assert row.created_datetime == self.created_datetime
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
                assert row.updated_datetime == self.updated_datetime
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
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
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
        histologic_progression: str | None = None
        history_of_tumor: str | None = None
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
        progression_or_recurrence: str | None = "True"
        progression_or_recurrence_anatomic_site: str | None = "Not Reported"
        progression_or_recurrence_type: str | None = "Unknown"
        recist_targeted_regions_number: int | None = None
        recist_targeted_regions_sum: float | None = None
        scan_tracer_used: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        timepoint_category: str | None = "Last Contact"
        treatment_emergent_adverse_event: str | None = None
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
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
            assert row.created_datetime == self.created_datetime
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
            assert row.updated_datetime == self.updated_datetime
            assert row.year_of_follow_up == self.year_of_follow_up
            assert tuple(row.imaging_anatomic_site) == self.imaging_anatomic_site
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.molecular_tests or (), self.molecular_tests or ())
            )
            assert all(
                e.assert_equals(r)
                for r, e in zip(
                    row.other_clinical_attributes or (), self.other_clinical_attributes or ()
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
        @dataclasses.dataclass(frozen=True)
        class Annotation:
            annotation_id: str | None = "annotation-0"
            case_id: str | None = "case-0"
            case_submitter_id: str | None = "TEST-UNIT-case-0"
            category: str | None = "Item is noncanonical"
            classification: str | None = "Notification"
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            creator: str | None = None
            entity_id: str | None = "entity-0"
            entity_submitter_id: str | None = "TEST-UNIT-entity-0"
            entity_type: str | None = "portion"
            legacy_created_datetime: str | None = "null"
            legacy_updated_datetime: str | None = None
            notes: str | None = "CPTAC Proteomics OV PNNL"
            state: str | None = "released"
            status: str | None = "Approved"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
                        case_submitter_id: str | None = "TEST-UNIT-case-0"
                        category: str | None = "Item is noncanonical"
                        classification: str | None = "Notification"
                        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                        creator: str | None = None
                        entity_id: str | None = "entity-0"
                        entity_submitter_id: str | None = "TEST-UNIT-entity-0"
                        entity_type: str | None = "portion"
                        legacy_created_datetime: str | None = "null"
                        legacy_updated_datetime: str | None = None
                        notes: str | None = "CPTAC Proteomics OV PNNL"
                        state: str | None = "released"
                        status: str | None = "Approved"
                        submitter_id: str | None = "TEST-UNIT-submitter-0"
                        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
                        center_type: str | None = "GSC"
                        code: str | None = "10"
                        name: str | None = "Baylor College of Medicine"
                        namespace: str | None = "hgsc.bcm.edu"
                        short_name: str | None = "BCM"

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
                    aliquot_quantity: float | None = 30.0
                    aliquot_volume: float | None = 60.0
                    amount: float | None = 65.0
                    analyte_type: str | None = "Repli-G (Qiagen) DNA"
                    annotations: tuple[Annotation, ...] | None = (Annotation(),)
                    center: Center | None = Center()
                    concentration: float | None = 0.5
                    created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                    no_matched_normal_low_pass_wgs: str | None = None
                    no_matched_normal_targeted_sequencing: str | None = "True"
                    no_matched_normal_wgs: str | None = "True"
                    no_matched_normal_wxs: str | None = "True"
                    selected_normal_low_pass_wgs: str | None = None
                    selected_normal_targeted_sequencing: str | None = "True"
                    selected_normal_wgs: str | None = "True"
                    selected_normal_wxs: str | None = "True"
                    source_center: str | None = "22"
                    state: str | None = "released"
                    submitter_id: str | None = "TEST-UNIT-submitter-0"
                    updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

                    def assert_equals(self, row: sql.Row) -> bool:
                        assert row
                        assert row.aliquot_id == self.aliquot_id
                        assert row.aliquot_quantity == self.aliquot_quantity
                        assert row.aliquot_volume == self.aliquot_volume
                        assert row.amount == self.amount
                        assert row.analyte_type == self.analyte_type
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
                            for r, e in zip(row.annotations or (), self.annotations or ())
                        )

                        return True

                @dataclasses.dataclass(frozen=True)
                class Annotation:
                    annotation_id: str | None = "annotation-0"
                    case_id: str | None = "case-0"
                    case_submitter_id: str | None = "TEST-UNIT-case-0"
                    category: str | None = "Item is noncanonical"
                    classification: str | None = "Notification"
                    created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                    creator: str | None = None
                    entity_id: str | None = "entity-0"
                    entity_submitter_id: str | None = "TEST-UNIT-entity-0"
                    entity_type: str | None = "portion"
                    legacy_created_datetime: str | None = "null"
                    legacy_updated_datetime: str | None = None
                    notes: str | None = "CPTAC Proteomics OV PNNL"
                    state: str | None = "released"
                    status: str | None = "Approved"
                    submitter_id: str | None = "TEST-UNIT-submitter-0"
                    updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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

                a260_a280_ratio: float | None = 1.9
                aliquots: tuple[Aliquot, ...] | None = (Aliquot(),)
                amount: float | None = 65.0
                analyte_id: str | None = "analyte-0"
                analyte_quantity: float | None = None
                analyte_type: str | None = "Repli-G (Qiagen) DNA"
                analyte_volume: float | None = 20.0
                annotations: tuple[Annotation, ...] | None = (Annotation(),)
                concentration: float | None = 0.5
                created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                dna_integrity_number: float | None = None
                experimental_protocol_type: str | None = "Repli-G"
                normal_tumor_genotype_snp_match: str | None = "True"
                ribosomal_rna_28s_18s_ratio: float | None = None
                rna_integrity_number: float | None = 8.6
                spectrophotometer_method: str | None = "UV Spec"
                state: str | None = "released"
                submitter_id: str | None = "TEST-UNIT-submitter-0"
                updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
                well_number: str | None = None

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.a260_a280_ratio == self.a260_a280_ratio
                    assert row.amount == self.amount
                    assert row.analyte_id == self.analyte_id
                    assert row.analyte_quantity == self.analyte_quantity
                    assert row.analyte_type == self.analyte_type
                    assert row.analyte_volume == self.analyte_volume
                    assert row.concentration == self.concentration
                    assert row.created_datetime == self.created_datetime
                    assert row.dna_integrity_number == self.dna_integrity_number
                    assert row.experimental_protocol_type == self.experimental_protocol_type
                    assert (
                        row.normal_tumor_genotype_snp_match
                        == self.normal_tumor_genotype_snp_match
                    )
                    assert row.ribosomal_rna_28s_18s_ratio == self.ribosomal_rna_28s_18s_ratio
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
                case_submitter_id: str | None = "TEST-UNIT-case-0"
                category: str | None = "Item is noncanonical"
                classification: str | None = "Notification"
                created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                creator: str | None = None
                entity_id: str | None = "entity-0"
                entity_submitter_id: str | None = "TEST-UNIT-entity-0"
                entity_type: str | None = "portion"
                legacy_created_datetime: str | None = "null"
                legacy_updated_datetime: str | None = None
                notes: str | None = "CPTAC Proteomics OV PNNL"
                state: str | None = "released"
                status: str | None = "Approved"
                submitter_id: str | None = "TEST-UNIT-submitter-0"
                updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
                center_type: str | None = "GSC"
                code: str | None = "10"
                name: str | None = "Baylor College of Medicine"
                namespace: str | None = "hgsc.bcm.edu"
                short_name: str | None = "BCM"

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
                    case_submitter_id: str | None = "TEST-UNIT-case-0"
                    category: str | None = "Item is noncanonical"
                    classification: str | None = "Notification"
                    created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                    creator: str | None = None
                    entity_id: str | None = "entity-0"
                    entity_submitter_id: str | None = "TEST-UNIT-entity-0"
                    entity_type: str | None = "portion"
                    legacy_created_datetime: str | None = "null"
                    legacy_updated_datetime: str | None = None
                    notes: str | None = "CPTAC Proteomics OV PNNL"
                    state: str | None = "released"
                    status: str | None = "Approved"
                    submitter_id: str | None = "TEST-UNIT-submitter-0"
                    updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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

                annotations: tuple[Annotation, ...] | None = (Annotation(),)
                bone_marrow_malignant_cells: str | None = None
                created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
                number_proliferating_cells: int | None = None
                percent_eosinophil_infiltration: float | None = None
                percent_follicular_component: float | None = None
                percent_granulocyte_infiltration: float | None = None
                percent_inflam_infiltration: float | None = None
                percent_lymphocyte_infiltration: float | None = 40.0
                percent_monocyte_infiltration: float | None = 20.0
                percent_necrosis: float | None = 5.0
                percent_neutrophil_infiltration: float | None = 20.0
                percent_normal_cells: float | None = 3.0
                percent_rhabdoid_features: float | None = None
                percent_sarcomatoid_features: float | None = None
                percent_stromal_cells: float | None = 10.0
                percent_tumor_cells: float | None = 85.0
                percent_tumor_nuclei: float | None = 95.0
                prostatic_chips_positive_count: float | None = None
                prostatic_chips_total_count: float | None = None
                prostatic_involvement_percent: float | None = None
                section_location: str | None = "Not Reported"
                slide_id: str | None = "slide-0"
                state: str | None = "released"
                submitter_id: str | None = "TEST-UNIT-submitter-0"
                tissue_microarray_coordinates: str | None = "A1,D3,E4"
                updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

                def assert_equals(self, row: sql.Row) -> bool:
                    assert row
                    assert row.bone_marrow_malignant_cells == self.bone_marrow_malignant_cells
                    assert row.created_datetime == self.created_datetime
                    assert row.number_proliferating_cells == self.number_proliferating_cells
                    assert (
                        row.percent_eosinophil_infiltration
                        == self.percent_eosinophil_infiltration
                    )
                    assert (
                        row.percent_follicular_component == self.percent_follicular_component
                    )
                    assert (
                        row.percent_granulocyte_infiltration
                        == self.percent_granulocyte_infiltration
                    )
                    assert row.percent_inflam_infiltration == self.percent_inflam_infiltration
                    assert (
                        row.percent_lymphocyte_infiltration
                        == self.percent_lymphocyte_infiltration
                    )
                    assert (
                        row.percent_monocyte_infiltration == self.percent_monocyte_infiltration
                    )
                    assert row.percent_necrosis == self.percent_necrosis
                    assert (
                        row.percent_neutrophil_infiltration
                        == self.percent_neutrophil_infiltration
                    )
                    assert row.percent_normal_cells == self.percent_normal_cells
                    assert row.percent_rhabdoid_features == self.percent_rhabdoid_features
                    assert (
                        row.percent_sarcomatoid_features == self.percent_sarcomatoid_features
                    )
                    assert row.percent_stromal_cells == self.percent_stromal_cells
                    assert row.percent_tumor_cells == self.percent_tumor_cells
                    assert row.percent_tumor_nuclei == self.percent_tumor_nuclei
                    assert (
                        row.prostatic_chips_positive_count
                        == self.prostatic_chips_positive_count
                    )
                    assert row.prostatic_chips_total_count == self.prostatic_chips_total_count
                    assert (
                        row.prostatic_involvement_percent == self.prostatic_involvement_percent
                    )
                    assert row.section_location == self.section_location
                    assert row.slide_id == self.slide_id
                    assert row.state == self.state
                    assert row.submitter_id == self.submitter_id
                    assert (
                        row.tissue_microarray_coordinates == self.tissue_microarray_coordinates
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
            created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
            creation_datetime: float | None = 1259539200.0
            is_ffpe: str | None = "True"
            portion_id: str | None = "portion-0"
            portion_number: str | None = "01"
            slides: tuple[Slide, ...] | None = (Slide(),)
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"
            weight: float | None = 105.0

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
                    e.assert_equals(r) for r, e in zip(row.analytes or (), self.analytes or ())
                )
                assert all(
                    e.assert_equals(r)
                    for r, e in zip(row.annotations or (), self.annotations or ())
                )
                assert all(
                    e.assert_equals(r) for r, e in zip(row.slides or (), self.slides or ())
                )

                return True

        annotations: tuple[Annotation, ...] | None = (Annotation(),)
        biospecimen_anatomic_site: str | None = "Bone"
        biospecimen_laterality: str | None = "Unknown"
        catalog_reference: str | None = None
        created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
        current_weight: float | None = 0.386
        days_to_collection: int | None = 807
        days_to_sample_procurement: int | None = -18
        diagnosis_pathologically_confirmed: str | None = "True"
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
        portions: tuple[Portion, ...] | None = (Portion(),)
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
        tumor_descriptor: str | None = "Not Applicable"
        updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.biospecimen_anatomic_site == self.biospecimen_anatomic_site
            assert row.biospecimen_laterality == self.biospecimen_laterality
            assert row.catalog_reference == self.catalog_reference
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
            assert row.tumor_descriptor == self.tumor_descriptor
            assert row.updated_datetime == self.updated_datetime
            assert all(
                e.assert_equals(r)
                for r, e in zip(row.annotations or (), self.annotations or ())
            )
            assert all(
                e.assert_equals(r) for r, e in zip(row.portions or (), self.portions or ())
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Summary:
        @dataclasses.dataclass(frozen=True)
        class DataCategory:
            data_category: str | None = "Simple Nucleotide Variation"
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
        file_size: int | None = 5777

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
                    row.experimental_strategies or (), self.experimental_strategies or ()
                )
            )

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

    aliquot_ids: tuple[str, ...] | None = ("aliquot-0",)
    analyte_ids: tuple[str, ...] | None = ("analyte-0",)
    annotations: tuple[Annotation, ...] | None = (Annotation(),)
    case_id: str | None = "case-0"
    consent_type: str | None = "Informed Consent"
    created_datetime: str | None = "2022-02-03T19:01:23.631381-06:00"
    days_to_consent: int | None = 15
    days_to_lost_to_followup: int | None = 600
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    diagnosis_ids: tuple[str, ...] | None = ("diagnosis-0",)
    disease_type: str | None = "Cystic, Mucinous and Serous Neoplasms"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    files: tuple[File, ...] | None = (File(),)
    follow_ups: tuple[FollowUp, ...] | None = (FollowUp(),)
    index_date: str | None = "Diagnosis"
    lost_to_followup: str | None = None
    portion_ids: tuple[str, ...] | None = ("portion-0",)
    primary_site: str | None = "Ovary"
    project: Project | None = Project()
    sample_ids: tuple[str, ...] | None = ("sample-0",)
    samples: tuple[Sample, ...] | None = (Sample(),)
    slide_ids: tuple[str, ...] | None = ("slide-0",)
    state: str | None = "released"
    submitter_aliquot_ids: tuple[str, ...] | None = ("TEST-UNIT-aliquot-0",)
    submitter_analyte_ids: tuple[str, ...] | None = ("TEST-UNIT-analyte-0",)
    submitter_diagnosis_ids: tuple[str, ...] | None = ("TEST-UNIT-diagnosis-0",)
    submitter_id: str | None = "TEST-UNIT-submitter-0"
    submitter_portion_ids: tuple[str, ...] | None = ("TEST-UNIT-portion-0",)
    submitter_sample_ids: tuple[str, ...] | None = ("TEST-UNIT-sample-0",)
    submitter_slide_ids: tuple[str, ...] | None = ("TEST-UNIT-slide-0",)
    summary: Summary | None = Summary()
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    updated_datetime: str | None = "2022-02-07T21:22:16.306502-06:00"

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
        assert (row.summary is None and self.summary is None) or (
            self.summary and self.summary.assert_equals(row.summary)
        )
        assert (row.tissue_source_site is None and self.tissue_source_site is None) or (
            self.tissue_source_site
            and self.tissue_source_site.assert_equals(row.tissue_source_site)
        )
        assert all(
            e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ())
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
        assert all(e.assert_equals(r) for r, e in zip(row.files or (), self.files or ()))
        assert all(
            e.assert_equals(r) for r, e in zip(row.follow_ups or (), self.follow_ups or ())
        )
        assert all(e.assert_equals(r) for r, e in zip(row.samples or (), self.samples or ()))

        return True
