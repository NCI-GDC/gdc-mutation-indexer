import dataclasses
from typing import Optional

from pyspark import sql


@dataclasses.dataclass(frozen=True)
class Hit:
    @dataclasses.dataclass(frozen=True)
    class Source:
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
                notes: Optional[str] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
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
                            created_datetime: Optional[str] = "2018-05-21T16:07:40.645885-05:00"
                            creator: Optional[str] = None
                            entity_id: Optional[str] = "entity-0"
                            entity_submitter_id: Optional[str] = "sub-entity-0"
                            entity_type: Optional[str] = "aliquot"
                            legacy_created_datetime: Optional[str] = None
                            legacy_updated_datetime: Optional[str] = None
                            notes: Optional[str] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
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
                            assert row.no_matched_normal_low_pass_wgs == self.no_matched_normal_low_pass_wgs
                            assert row.no_matched_normal_targeted_sequencing == self.no_matched_normal_targeted_sequencing
                            assert row.no_matched_normal_wgs == self.no_matched_normal_wgs
                            assert row.no_matched_normal_wxs == self.no_matched_normal_wxs
                            assert row.selected_normal_low_pass_wgs == self.selected_normal_low_pass_wgs
                            assert row.selected_normal_targeted_sequencing == self.selected_normal_targeted_sequencing
                            assert row.selected_normal_wgs == self.selected_normal_wgs
                            assert row.selected_normal_wxs == self.selected_normal_wxs
                            assert row.source_center == self.source_center
                            assert row.state == self.state
                            assert row.submitter_id == self.submitter_id
                            assert row.updated_datetime == self.updated_datetime
                            assert (
                                (row.center is None and self.center is None) 
                                or (self.center and self.center.assert_equals(row.center))
                            )
                            assert all(e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ()))

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
                        notes: Optional[str] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
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
                        assert row.experimental_protocol_type == self.experimental_protocol_type
                        assert row.normal_tumor_genotype_snp_match == self.normal_tumor_genotype_snp_match
                        assert row.ribosomal_rna_28s_16s_ratio == self.ribosomal_rna_28s_16s_ratio
                        assert row.rna_integrity_number == self.rna_integrity_number
                        assert row.spectrophotometer_method == self.spectrophotometer_method
                        assert row.state == self.state
                        assert row.submitter_id == self.submitter_id
                        assert row.updated_datetime == self.updated_datetime
                        assert row.well_number == self.well_number
                        assert all(e.assert_equals(r) for r, e in zip(row.aliquots or (), self.aliquots or ()))
                        assert all(e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ()))

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
                    notes: Optional[str] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
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
                        notes: Optional[str] = "This is the correct replacement barcode for RNA aliquot UUID: 50F23D00-F6FD-4B3D-AED2-0705F7AE6A17, which is a replacement aliquot for UUID: 60770e58-3b35-4222-97e9-d92e973f0203 that was found to have inconclusive identity (RNA only).   Note that this replacement aliquot is derived from a different portion than the DNA."
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
                        assert row.bone_marrow_malignant_cells == self.bone_marrow_malignant_cells
                        assert row.created_datetime == self.created_datetime
                        assert row.number_proliferating_cells == self.number_proliferating_cells
                        assert row.percent_eosinophil_infiltration == self.percent_eosinophil_infiltration
                        assert row.percent_follicular_component == self.percent_follicular_component
                        assert row.percent_granulocyte_infiltration == self.percent_granulocyte_infiltration
                        assert row.percent_inflam_infiltration == self.percent_inflam_infiltration
                        assert row.percent_lymphocyte_infiltration == self.percent_lymphocyte_infiltration
                        assert row.percent_monocyte_infiltration == self.percent_monocyte_infiltration
                        assert row.percent_necrosis == self.percent_necrosis
                        assert row.percent_neutrophil_infiltration == self.percent_neutrophil_infiltration
                        assert row.percent_normal_cells == self.percent_normal_cells
                        assert row.percent_rhabdoid_features == self.percent_rhabdoid_features
                        assert row.percent_sarcomatoid_features == self.percent_sarcomatoid_features
                        assert row.percent_stromal_cells == self.percent_stromal_cells
                        assert row.percent_tumor_cells == self.percent_tumor_cells
                        assert row.percent_tumor_nuclei == self.percent_tumor_nuclei
                        assert row.prostatic_chips_positive_count == self.prostatic_chips_positive_count
                        assert row.prostatic_chips_total_count == self.prostatic_chips_total_count
                        assert row.prostatic_involvement_percent == self.prostatic_involvement_percent
                        assert row.section_location == self.section_location
                        assert row.slide_id == self.slide_id
                        assert row.state == self.state
                        assert row.submitter_id == self.submitter_id
                        assert row.tissue_microarray_coordinates == self.tissue_microarray_coordinates
                        assert row.updated_datetime == self.updated_datetime
                        assert all(e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ()))

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
                    assert (
                        (row.center is None and self.center is None) 
                        or (self.center and self.center.assert_equals(row.center))
                    )
                    assert all(e.assert_equals(r) for r, e in zip(row.analytes or (), self.analytes or ()))
                    assert all(e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ()))
                    assert all(e.assert_equals(r) for r, e in zip(row.slides or (), self.slides or ()))

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
                assert row.diagnosis_pathologically_confirmed == self.diagnosis_pathologically_confirmed
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
                assert row.state == self.state
                assert row.submitter_id == self.submitter_id
                assert row.time_between_clamping_and_freezing == self.time_between_clamping_and_freezing
                assert row.time_between_excision_and_freezing == self.time_between_excision_and_freezing
                assert row.tissue_collection_type == self.tissue_collection_type
                assert row.tissue_type == self.tissue_type
                assert row.tumor_code == self.tumor_code
                assert row.tumor_code_id == self.tumor_code_id
                assert row.tumor_descriptor == self.tumor_descriptor
                assert row.updated_datetime == self.updated_datetime
                assert all(e.assert_equals(r) for r, e in zip(row.annotations or (), self.annotations or ()))
                assert all(e.assert_equals(r) for r, e in zip(row.portions or (), self.portions or ()))

                return True

        case_id: Optional[str] = "case-0"
        samples: Optional[tuple[Sample, ...]] = (Sample(),)

        def assert_equals(self, row: sql.Row) -> bool:
            assert row
            assert row.case_id == self.case_id
            assert all(e.assert_equals(r) for r, e in zip(row.samples or (), self.samples or ()))

            return True

    _id: Optional[str] = "case-0"
    _source: Optional[Source] = Source()

    def assert_equals(self, row: sql.Row) -> bool:
        assert row
        assert row._id == self._id
        assert (
            (row._source is None and self._source is None) 
            or (self._source and self._source.assert_equals(row._source))
        )

        return True

