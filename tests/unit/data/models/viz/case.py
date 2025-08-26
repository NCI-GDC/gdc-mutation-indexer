import dataclasses


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
            largest_extrapelvic_peritoneal_focus: None | (str) = "Macroscopic (2cm or less)"
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
    primary_site: None | (str) = "['Unknown', 'Hematopoietic and reticuloendothelial systems']"
    project: Project | None = Project()
    samples: tuple[Sample, ...] | None = (Sample(),)
    state: str | None = "released"
    submitter_id: str | None = "1385db59-6d8e-4d6e-a8aa-4ddee67f9289"
    tissue_source_site: TissueSourceSite | None = TissueSourceSite()
    available_variation_data: tuple[str, ...] | None = None
