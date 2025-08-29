import dataclasses

from pyspark import sql


@dataclasses.dataclass(frozen=True)
class Case:
    @dataclasses.dataclass(frozen=True)
    class Demographic:
        age_at_index: int | None = 85
        age_is_obfuscated: str | None = "True"
        cause_of_death: str | None = "Cancer Related"
        days_to_birth: int | None = -31232
        days_to_death: int | None = 1324
        demographic_id: str | None = "demographic-0"
        ethnicity: str | None = "not hispanic or latino"
        gender: str | None = "female"
        race: str | None = "white"
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
            assert row.days_to_birth == self.days_to_birth
            assert row.days_to_death == self.days_to_death
            assert row.demographic_id == self.demographic_id
            assert row.ethnicity == self.ethnicity
            assert row.gender == self.gender
            assert row.race == self.race
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
            anaplasia_present: str | None = None
            anaplasia_present_type: str | None = "Unknown"
            bone_marrow_malignant_cells: str | None = None
            breslow_thickness: float | None = 3.0
            circumferential_resection_margin: float | None = 6.0
            columnar_mucosa_present: str | None = None
            dysplasia_degree: str | None = None
            dysplasia_type: str | None = None
            greatest_tumor_dimension: float | None = 3.0
            gross_tumor_weight: float | None = 300.0
            largest_extrapelvic_peritoneal_focus: str | None = "Macroscopic (greater than 2cm)"
            lymph_node_involved_site: str | None = "Pelvis, NOS"
            lymph_node_involvement: str | None = "Positive"
            lymph_nodes_positive: int | None = 1
            lymph_nodes_tested: int | None = 7
            lymphatic_invasion_present: str | None = "True"
            margin_status: str | None = "Uninvolved"
            metaplasia_present: str | None = None
            morphologic_architectural_pattern: str | None = "Cohesive"
            non_nodal_regional_disease: str | None = None
            non_nodal_tumor_deposits: str | None = "True"
            number_proliferating_cells: int | None = None
            pathology_detail_id: str | None = "pathology-detail-0"
            percent_tumor_invasion: float | None = 8.3
            perineural_invasion_present: str | None = None
            peripancreatic_lymph_nodes_positive: str | None = "1-3"
            peripancreatic_lymph_nodes_tested: int | None = 77
            prostatic_chips_positive_count: float | None = None
            prostatic_chips_total_count: float | None = None
            prostatic_involvement_percent: float | None = None
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            transglottic_extension: str | None = "Present"
            tumor_largest_dimension_diameter: float | None = 3.0
            vascular_invasion_present: str | None = "True"
            vascular_invasion_type: str | None = "No Vascular Invasion"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.anaplasia_present == self.anaplasia_present
                assert row.anaplasia_present_type == self.anaplasia_present_type
                assert row.bone_marrow_malignant_cells == self.bone_marrow_malignant_cells
                assert row.breslow_thickness == self.breslow_thickness
                assert (
                    row.circumferential_resection_margin
                    == self.circumferential_resection_margin
                )
                assert row.columnar_mucosa_present == self.columnar_mucosa_present
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
                assert row.non_nodal_regional_disease == self.non_nodal_regional_disease
                assert row.non_nodal_tumor_deposits == self.non_nodal_tumor_deposits
                assert row.number_proliferating_cells == self.number_proliferating_cells
                assert row.pathology_detail_id == self.pathology_detail_id
                assert row.percent_tumor_invasion == self.percent_tumor_invasion
                assert row.perineural_invasion_present == self.perineural_invasion_present
                assert (
                    row.peripancreatic_lymph_nodes_positive
                    == self.peripancreatic_lymph_nodes_positive
                )
                assert (
                    row.peripancreatic_lymph_nodes_tested
                    == self.peripancreatic_lymph_nodes_tested
                )
                assert (
                    row.prostatic_chips_positive_count == self.prostatic_chips_positive_count
                )
                assert row.prostatic_chips_total_count == self.prostatic_chips_total_count
                assert row.prostatic_involvement_percent == self.prostatic_involvement_percent
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.transglottic_extension == self.transglottic_extension
                assert (
                    row.tumor_largest_dimension_diameter
                    == self.tumor_largest_dimension_diameter
                )
                assert row.vascular_invasion_present == self.vascular_invasion_present
                assert row.vascular_invasion_type == self.vascular_invasion_type

                return True

        @dataclasses.dataclass(frozen=True)
        class Treatment:
            chemo_concurrent_to_radiation: str | None = "True"
            days_to_treatment_end: int | None = 1189
            days_to_treatment_start: int | None = 1140
            initial_disease_status: str | None = "Recurrent Disease"
            number_of_cycles: int | None = 3
            regimen_or_line_of_therapy: str | None = "TIP"
            state: str | None = "released"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            therapeutic_agents: str | None = "Gemcitabine Hydrochloride"
            treatment_dose: int | None = 6952
            treatment_frequency: str | None = "Once Weekly"
            treatment_id: str | None = "treatment-0"
            treatment_intent_type: str | None = "Adjuvant"
            treatment_or_therapy: str | None = None
            treatment_outcome: str | None = "Not Reported"
            treatment_type: str | None = "Radiation Therapy, NOS"

            def assert_equals(self, row: sql.Row) -> bool:
                assert row
                assert row.chemo_concurrent_to_radiation == self.chemo_concurrent_to_radiation
                assert row.days_to_treatment_end == self.days_to_treatment_end
                assert row.days_to_treatment_start == self.days_to_treatment_start
                assert row.initial_disease_status == self.initial_disease_status
                assert row.number_of_cycles == self.number_of_cycles
                assert row.regimen_or_line_of_therapy == self.regimen_or_line_of_therapy
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.therapeutic_agents == self.therapeutic_agents
                assert row.treatment_dose == self.treatment_dose
                assert row.treatment_frequency == self.treatment_frequency
                assert row.treatment_id == self.treatment_id
                assert row.treatment_intent_type == self.treatment_intent_type
                assert row.treatment_or_therapy == self.treatment_or_therapy
                assert row.treatment_outcome == self.treatment_outcome
                assert row.treatment_type == self.treatment_type

                return True

        age_at_diagnosis: int | None = 31232
        ajcc_clinical_m: str | None = "MX"
        ajcc_clinical_n: str | None = "N0"
        ajcc_clinical_stage: str | None = "Stage IV"
        ajcc_clinical_t: str | None = "T1"
        ajcc_pathologic_m: str | None = "MX"
        ajcc_pathologic_n: str | None = "N0"
        ajcc_pathologic_stage: str | None = "Stage IB"
        ajcc_pathologic_t: str | None = "T1c"
        ajcc_staging_system_edition: str | None = "7th"
        ann_arbor_b_symptoms: str | None = "True"
        ann_arbor_clinical_stage: str | None = "Stage III"
        ann_arbor_extranodal_involvement: str | None = None
        ann_arbor_pathologic_stage: str | None = "Stage III"
        burkitt_lymphoma_clinical_variant: str | None = "Endemic"
        classification_of_tumor: str | None = "primary"
        cog_renal_stage: str | None = "Stage I"
        days_to_diagnosis: int | None = 505
        days_to_last_follow_up: float | None = 84.0
        days_to_last_known_disease_status: float | None = 510.0
        days_to_recurrence: float | None = 505.0
        diagnosis_id: str | None = "diagnosis-0"
        esophageal_columnar_dysplasia_degree: str | None = "High Grade Dysplasia"
        esophageal_columnar_metaplasia_present: str | None = "True"
        figo_stage: str | None = "Stage IIIC"
        figo_staging_edition_year: str | None = "2009"
        gastric_esophageal_junction_involvement: str | None = "True"
        goblet_cells_columnar_mucosa_present: str | None = "True"
        icd_10_code: str | None = "C56.9"
        igcccg_stage: str | None = "Good Prognosis"
        inss_stage: str | None = "Stage 4"
        international_prognostic_index: str | None = "High Risk"
        iss_stage: str | None = "I"
        last_known_disease_status: str | None = "Distant met recurrence/progression"
        laterality: str | None = "Left"
        masaoka_stage: str | None = "Stage IIb"
        metastasis_at_diagnosis: str | None = "No Metastasis"
        method_of_diagnosis: str | None = "Surgical Resection"
        morphology: str | None = "8441/3"
        pathology_details: tuple[PathologyDetail, ...] | None = (PathologyDetail(),)
        primary_diagnosis: str | None = "Serous cystadenocarcinoma, NOS"
        primary_gleason_grade: str | None = "Pattern 3"
        prior_malignancy: str | None = "not reported"
        prior_treatment: str | None = None
        progression_or_recurrence: str | None = "True"
        residual_disease: str | None = "R0"
        secondary_gleason_grade: str | None = "Pattern 4"
        site_of_resection_or_biopsy: str | None = "Ovary"
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        synchronous_malignancy: str | None = "Not Reported"
        tissue_or_organ_of_origin: str | None = "Ovary"
        treatments: tuple[Treatment, ...] | None = (Treatment(),)
        tumor_grade: str | None = "G3"
        year_of_diagnosis: int | None = 2009

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
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
            assert row.ann_arbor_clinical_stage == self.ann_arbor_clinical_stage
            assert (
                row.ann_arbor_extranodal_involvement == self.ann_arbor_extranodal_involvement
            )
            assert row.ann_arbor_pathologic_stage == self.ann_arbor_pathologic_stage
            assert (
                row.burkitt_lymphoma_clinical_variant == self.burkitt_lymphoma_clinical_variant
            )
            assert row.classification_of_tumor == self.classification_of_tumor
            assert row.cog_renal_stage == self.cog_renal_stage
            assert row.days_to_diagnosis == self.days_to_diagnosis
            assert row.days_to_last_follow_up == self.days_to_last_follow_up
            assert (
                row.days_to_last_known_disease_status == self.days_to_last_known_disease_status
            )
            assert row.days_to_recurrence == self.days_to_recurrence
            assert row.diagnosis_id == self.diagnosis_id
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
                row.gastric_esophageal_junction_involvement
                == self.gastric_esophageal_junction_involvement
            )
            assert (
                row.goblet_cells_columnar_mucosa_present
                == self.goblet_cells_columnar_mucosa_present
            )
            assert row.icd_10_code == self.icd_10_code
            assert row.igcccg_stage == self.igcccg_stage
            assert row.inss_stage == self.inss_stage
            assert row.international_prognostic_index == self.international_prognostic_index
            assert row.iss_stage == self.iss_stage
            assert row.last_known_disease_status == self.last_known_disease_status
            assert row.laterality == self.laterality
            assert row.masaoka_stage == self.masaoka_stage
            assert row.metastasis_at_diagnosis == self.metastasis_at_diagnosis
            assert row.method_of_diagnosis == self.method_of_diagnosis
            assert row.morphology == self.morphology
            assert row.primary_diagnosis == self.primary_diagnosis
            assert row.primary_gleason_grade == self.primary_gleason_grade
            assert row.prior_malignancy == self.prior_malignancy
            assert row.prior_treatment == self.prior_treatment
            assert row.progression_or_recurrence == self.progression_or_recurrence
            assert row.residual_disease == self.residual_disease
            assert row.secondary_gleason_grade == self.secondary_gleason_grade
            assert row.site_of_resection_or_biopsy == self.site_of_resection_or_biopsy
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.synchronous_malignancy == self.synchronous_malignancy
            assert row.tissue_or_organ_of_origin == self.tissue_or_organ_of_origin
            assert row.tumor_grade == self.tumor_grade
            assert row.year_of_diagnosis == self.year_of_diagnosis
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
        alcohol_days_per_week: float | None = 7.0
        alcohol_history: str | None = "True"
        alcohol_intensity: str | None = "Drinker"
        cigarettes_per_day: float | None = 20.0
        exposure_id: str | None = "exposure-0"
        pack_years_smoked: float | None = 83.0
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        tobacco_smoking_onset_year: int | None = 1946
        tobacco_smoking_quit_year: int | None = 1981
        tobacco_smoking_status: str | None = "Current Smoker"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.alcohol_days_per_week == self.alcohol_days_per_week
            assert row.alcohol_history == self.alcohol_history
            assert row.alcohol_intensity == self.alcohol_intensity
            assert row.cigarettes_per_day == self.cigarettes_per_day
            assert row.exposure_id == self.exposure_id
            assert row.pack_years_smoked == self.pack_years_smoked
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id
            assert row.tobacco_smoking_onset_year == self.tobacco_smoking_onset_year
            assert row.tobacco_smoking_quit_year == self.tobacco_smoking_quit_year
            assert row.tobacco_smoking_status == self.tobacco_smoking_status

            return True

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        family_history_id: str | None = "family-history-0"
        relationship_age_at_diagnosis: float | None = None
        relationship_gender: str | None = "female"
        relationship_primary_diagnosis: str | None = "Lung Cancer"
        relationship_type: str | None = "Sibling"
        relative_with_cancer_history: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.family_history_id == self.family_history_id
            assert row.relationship_age_at_diagnosis == self.relationship_age_at_diagnosis
            assert row.relationship_gender == self.relationship_gender
            assert row.relationship_primary_diagnosis == self.relationship_primary_diagnosis
            assert row.relationship_type == self.relationship_type
            assert row.relative_with_cancer_history == self.relative_with_cancer_history
            assert row.state == self.state
            assert row.submitter_id == self.submitter_id

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

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.dbgap_accession_number == self.dbgap_accession_number
            assert row.intended_release_date == self.intended_release_date
            assert row.name == self.name
            assert row.project_id == self.project_id
            assert tuple(row.disease_type) == self.disease_type
            assert tuple(row.primary_site) == self.primary_site
            assert (row.program is None and self.program is None) or (
                self.program and self.program.assert_equals(row.program)
            )

            return True

    @dataclasses.dataclass(frozen=True)
    class Sample:
        preservation_method: str | None = "Unknown"
        sample_type: str | None = "Blood Derived Normal"
        specimen_type: str | None = "Peripheral Blood NOS"
        tissue_type: str | None = "Normal"
        tumor_descriptor: str | None = "Not Applicable"

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.preservation_method == self.preservation_method
            assert row.sample_type == self.sample_type
            assert row.specimen_type == self.specimen_type
            assert row.tissue_type == self.tissue_type
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
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    disease_type: str | None = "Cystic, Mucinous and Serous Neoplasms"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    index_date: str | None = "Diagnosis"
    lost_to_followup: str | None = None
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
        assert all(e.assert_equals(r) for r, e in zip(row.samples or (), self.samples or ()))

        return True
