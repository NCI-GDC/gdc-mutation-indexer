import dataclasses


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
            treatment_type_administered: str | None = None

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
        diagnosis_is_primary_disease: str | None = "True"
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

    @dataclasses.dataclass(frozen=True)
    class FamilyHistory:
        family_history_id: str | None = "family-history-0"
        relationship_age_at_diagnosis: float | None = None
        relationship_primary_diagnosis: str | None = "Lung Cancer"
        relationship_type: str | None = "Sibling"
        relative_with_cancer_history: str | None = None
        state: str | None = "released"
        submitter_id: str | None = "TEST-UNIT-submitter-0"

    @dataclasses.dataclass(frozen=True)
    class FollowUp:
        @dataclasses.dataclass(frozen=True)
        class MolecularTest:
            gene_symbol: str | None = "KRAS"
            molecular_analysis_method: str | None = "Not Reported"
            molecular_test_id: str | None = "molecular-test-0"
            submitter_id: str | None = "TEST-UNIT-submitter-0"
            test_result: str | None = "Negative"

        @dataclasses.dataclass(frozen=True)
        class OtherClinicalAttribute:
            other_clinical_attribute_id: str | None = "other-clinical-attribute-0"
            submitter_id: str | None = "TEST-UNIT-submitter-0"

        days_to_follow_up: int | None = 84
        follow_up_id: str | None = "follow-up-0"
        molecular_tests: tuple[MolecularTest, ...] | None = (MolecularTest(),)
        other_clinical_attributes: tuple[OtherClinicalAttribute, ...] | None = (
            OtherClinicalAttribute(),
        )
        submitter_id: str | None = "TEST-UNIT-submitter-0"

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
        @dataclasses.dataclass(frozen=True)
        class Portion:
            @dataclasses.dataclass(frozen=True)
            class Analyte:
                @dataclasses.dataclass(frozen=True)
                class Aliquot:
                    aliquot_id: str | None = "aliquot-0"
                    submitter_id: str | None = "TEST-UNIT-submitter-0"

                aliquots: tuple[Aliquot, ...] | None = (Aliquot(),)
                analyte_id: str | None = "analyte-0"
                analyte_type: str | None = "Repli-G (Qiagen) DNA"
                submitter_id: str | None = "TEST-UNIT-submitter-0"

            @dataclasses.dataclass(frozen=True)
            class Slide:
                section_location: str | None = "Not Reported"
                slide_id: str | None = "slide-0"
                submitter_id: str | None = "TEST-UNIT-submitter-0"

            analytes: tuple[Analyte, ...] | None = (Analyte(),)
            portion_id: str | None = "portion-0"
            slides: tuple[Slide, ...] | None = (Slide(),)
            submitter_id: str | None = "TEST-UNIT-submitter-0"

        portions: tuple[Portion, ...] | None = (Portion(),)
        preservation_method: str | None = "Unknown"
        sample_id: str | None = "sample-0"
        sample_type: str | None = "Blood Derived Normal"
        specimen_type: str | None = "Peripheral Blood NOS"
        submitter_id: str | None = "TEST-UNIT-submitter-0"
        tissue_type: str | None = "Normal"
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
    demographic: Demographic | None = Demographic()
    diagnoses: tuple[Diagnosis, ...] | None = (Diagnosis(),)
    disease_type: str | None = "Cystic, Mucinous and Serous Neoplasms"
    exposures: tuple[Exposure, ...] | None = (Exposure(),)
    family_histories: tuple[FamilyHistory, ...] | None = (FamilyHistory(),)
    follow_ups: tuple[FollowUp, ...] | None = (FollowUp(),)
    index_date: str | None = "Diagnosis"
    lost_to_followup: str | None = None
    primary_site: str | None = "Ovary"
    project: Project | None = Project()
    samples: tuple[Sample, ...] | None = (Sample(),)
    state: str | None = "released"
    submitter_id: str | None = "TEST-UNIT-submitter-0"
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    available_variation_data: tuple[str, ...] | None = None
