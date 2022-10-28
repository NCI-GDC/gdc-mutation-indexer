import dataclasses
from distutils import util
from typing import Iterable, Optional, Tuple

import more_itertools
from pyspark import sql

from tests.unit import utils


@dataclasses.dataclass(frozen=True)
class Center:
    center_id: Optional[str] = None
    center_type: Optional[str] = None
    code: Optional[str] = None
    name: Optional[str] = None
    namespace: Optional[str] = None
    short_name: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Aliquot:
    aliquot_id: Optional[str] = "3e4e1eca-35d5-4556-94a3-b230e1bf7c6e"
    aliquot_quantity: Optional[float] = 2.4
    aliquot_volume: Optional[float] = 80.0
    amount: Optional[float] = 55.6
    analyte_type: Optional[str] = None
    analyte_type_id: Optional[str] = None
    center: Center = Center()
    concentration: Optional[float] = 0.03
    no_matched_normal_low_pass_wgs: Optional[str] = None
    no_matched_normal_targeted_sequencing: Optional[str] = None
    no_matched_normal_wgs: Optional[str] = None
    no_matched_normal_wxs: Optional[str] = None
    selected_normal_low_pass_wgs: Optional[str] = None
    selected_normal_targeted_sequencing: Optional[str] = None
    selected_normal_wgs: Optional[str] = None
    selected_normal_wxs: Optional[str] = None
    source_center: Optional[str] = "23"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18-06A-21D-A82H-36"


@dataclasses.dataclass(frozen=True)
class Analyte:
    a260_a280_ratio: Optional[float] = 4885.555
    aliquots: Tuple[Aliquot, ...] = (Aliquot(),)
    amount: Optional[float] = 0.654
    analyte_id: Optional[str] = "105414b5-f9f3-4f9e-aea7-ea29dc823dbd"
    analyte_quantity: Optional[float] = 8.23
    analyte_type: Optional[str] = "DNA"
    analyte_type_id: Optional[str] = "D"
    analyte_volume: Optional[float] = 85.3
    concentration: Optional[float] = 0.14
    experimental_protocol_type: Optional[str] = "aDNA Preparation Type"
    normal_tumor_genotype_snp_match: Optional[str] = "Yes"
    ribosomal_rna_28s_16s_ratio: Optional[float] = 845.2
    rna_integrity_number: Optional[float] = 0.0012
    spectrophotometer_method: Optional[str] = "PicoGreen"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18-06A-21D"
    well_number: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Slide:
    bone_marrow_malignant_cells: Optional[str] = None
    number_proliferating_cells: Optional[int] = None
    percent_eosinophil_infiltration: Optional[float] = 8.36
    percent_follicular_component: Optional[float] = 74.21
    percent_granulocyte_infiltration: Optional[float] = 3.5
    percent_inflam_infiltration: Optional[float] = 6.66
    percent_lymphocyte_infiltration: Optional[float] = 5.0
    percent_monocyte_infiltration: Optional[float] = 0.0
    percent_necrosis: Optional[float] = 5.0
    percent_neutrophil_infiltration: Optional[float] = 0.0
    percent_normal_cells: Optional[float] = 5.0
    percent_rhabdoid_features: Optional[float] = 96.58
    percent_sarcomatoid_features: Optional[float] = 72.0005
    percent_stromal_cells: Optional[float] = 55.0
    percent_tumor_cells: Optional[float] = 35.0
    percent_tumor_nuclei: Optional[float] = 35.0
    prostatic_chips_positive_count: Optional[float] = 0.2452
    prostatic_chips_total_count: Optional[float] = 0.000236
    prostatic_involvement_percent: Optional[float] = 845.0
    section_location: Optional[str] = "TOP"
    slide_id: Optional[str] = "35a22ce9-8815-4466-b63b-74063eedfaab"
    submitter_id: Optional[str] = "HCM-BROD-0001-C18-06A-02-S1-HE"
    tissue_microarray_coordinates: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Portion:
    analytes: Tuple[Analyte, ...] = (Analyte(),)
    center: Center = Center()
    creation_datetime: Optional[float] = 1595203200.0
    is_ffpe: Optional[str] = False
    portion_id: Optional[str] = "ddea617a-e13c-4b8c-9c32-ff9c837acc28"
    portion_number: Optional[str] = None
    slides: Tuple[Slide, ...] = (Slide(),)
    submitter_id: Optional[str] = "HCM-BROD-0001-C18-06A-21"
    weight: Optional[float] = 30.0


@dataclasses.dataclass(frozen=True)
class Sample:
    biospecimen_anatomic_site: Optional[str] = None
    biospecimen_laterality: Optional[str] = None
    catalog_reference: Optional[str] = None
    composition: Optional[str] = "Solid Tissue"
    current_weight: Optional[float] = 7.6
    days_to_collection: Optional[int] = None
    days_to_sample_procurement: Optional[int] = None
    diagnosis_pathologically_confirmed: Optional[str] = None
    distance_normal_to_tumor: Optional[str] = None
    distributor_reference: Optional[str] = None
    freezing_method: Optional[str] = None
    growth_rate: Optional[int] = None
    initial_weight: Optional[float] = 8.3
    intermediate_dimension: Optional[float] = 15.5
    is_ffpe: Optional[str] = False
    longest_dimension: Optional[float] = 17.1
    method_of_sample_procurement: Optional[str] = None
    oct_embedded: Optional[str] = "false"
    passage_count: Optional[int] = None
    pathology_report_uuid: Optional[str] = None
    portions: Tuple[Portion, ...] = (Portion(),)
    preservation_method: Optional[str] = "Frozen"
    sample_id: Optional[str] = "04bbf585-fd12-4c41-b6b1-2214a5826286"
    sample_ordinal: Optional[int] = 1
    sample_type: Optional[str] = "Metastatic"
    sample_type_id: Optional[str] = "06"
    shortest_dimension: Optional[float] = 12.3
    submitter_id: Optional[str] = "HCM-BROD-0001-C18-06A"
    time_between_clamping_and_freezing: Optional[float] = 85.2
    time_between_excision_and_freezing: Optional[float] = 78.6
    tissue_collection_type: Optional[str] = None
    tissue_type: Optional[str] = "Tumor"
    tumor_code: Optional[str] = None
    tumor_code_id: Optional[str] = None
    tumor_descriptor: Optional[str] = "Metastatic"


@dataclasses.dataclass(frozen=True)
class Source:
    case_id: Optional[str] = "case-0"
    samples: Tuple[Sample, ...] = (Sample(),)


@dataclasses.dataclass(frozen=True)
class Hit:
    _id: Optional[str] = "case-0"
    _source: Source = Source()


def assert_center_translated(result_center: sql.Row, center: Center) -> None:
    assert result_center.center_id == center.center_id
    assert result_center.center_type == center.center_type
    assert result_center.code == center.code
    assert result_center.name == center.name
    assert result_center.namespace == center.namespace
    assert result_center.short_name == center.short_name


def assert_aliquots_translated(
    result_aliquots: Iterable[sql.Row], aliquots: Iterable[Aliquot]
) -> None:
    result_aliquot = more_itertools.one(result_aliquots)
    aliquot = more_itertools.one(aliquots)

    assert result_aliquot.aliquot_id == aliquot.aliquot_id
    utils.assert_float_equal(result_aliquot.aliquot_quantity, aliquot.aliquot_quantity)
    utils.assert_float_equal(result_aliquot.aliquot_volume, aliquot.aliquot_volume)
    utils.assert_float_equal(result_aliquot.amount, aliquot.amount)
    assert result_aliquot.analyte_type == aliquot.analyte_type
    assert result_aliquot.analyte_type_id == aliquot.analyte_type_id
    utils.assert_float_equal(result_aliquot.concentration, aliquot.concentration)
    assert (
        result_aliquot.no_matched_normal_low_pass_wgs
        == aliquot.no_matched_normal_low_pass_wgs
    )
    assert (
        result_aliquot.no_matched_normal_targeted_sequencing
        == aliquot.no_matched_normal_targeted_sequencing
    )
    assert result_aliquot.no_matched_normal_wgs == aliquot.no_matched_normal_wgs
    assert result_aliquot.no_matched_normal_wxs == aliquot.no_matched_normal_wxs
    assert (
        result_aliquot.selected_normal_low_pass_wgs
        == aliquot.selected_normal_low_pass_wgs
    )
    assert (
        result_aliquot.selected_normal_targeted_sequencing
        == aliquot.selected_normal_targeted_sequencing
    )
    assert result_aliquot.selected_normal_wgs == aliquot.selected_normal_wgs
    assert result_aliquot.selected_normal_wxs == aliquot.selected_normal_wxs
    assert result_aliquot.source_center == aliquot.source_center
    assert result_aliquot.submitter_id == aliquot.submitter_id

    assert_center_translated(result_aliquot.center, aliquot.center)


def assert_analytes_translated(
    result_analytes: Iterable[sql.Row], analytes: Iterable[Analyte]
) -> None:
    result_analyte = more_itertools.one(result_analytes)
    analyte = more_itertools.one(analytes)

    utils.assert_float_equal(result_analyte.a260_a280_ratio, analyte.a260_a280_ratio)
    utils.assert_float_equal(result_analyte.amount, analyte.amount)
    assert result_analyte.analyte_id == analyte.analyte_id
    utils.assert_float_equal(result_analyte.analyte_quantity, analyte.analyte_quantity)
    assert result_analyte.analyte_type == analyte.analyte_type
    assert result_analyte.analyte_type_id == analyte.analyte_type_id
    utils.assert_float_equal(result_analyte.analyte_volume, analyte.analyte_volume)
    utils.assert_float_equal(result_analyte.concentration, analyte.concentration)
    assert (
        result_analyte.experimental_protocol_type == analyte.experimental_protocol_type
    )
    assert (
        result_analyte.normal_tumor_genotype_snp_match
        == analyte.normal_tumor_genotype_snp_match
    )
    utils.assert_float_equal(
        result_analyte.ribosomal_rna_28s_16s_ratio, analyte.ribosomal_rna_28s_16s_ratio
    )
    utils.assert_float_equal(
        result_analyte.rna_integrity_number, analyte.rna_integrity_number
    )
    assert result_analyte.spectrophotometer_method == analyte.spectrophotometer_method
    assert result_analyte.submitter_id == analyte.submitter_id
    assert result_analyte.well_number == analyte.well_number

    assert_aliquots_translated(result_analyte.aliquots, analyte.aliquots)


def assert_slides_translated(
    result_slides: Iterable[sql.Row], slides: Iterable[Slide]
) -> None:
    result_slide = more_itertools.one(result_slides)
    slide = more_itertools.one(slides)

    assert result_slide.bone_marrow_malignant_cells == slide.bone_marrow_malignant_cells
    assert result_slide.number_proliferating_cells == slide.number_proliferating_cells
    utils.assert_float_equal(
        result_slide.percent_eosinophil_infiltration,
        slide.percent_eosinophil_infiltration,
    )
    utils.assert_float_equal(
        result_slide.percent_follicular_component, slide.percent_follicular_component
    )
    utils.assert_float_equal(
        result_slide.percent_granulocyte_infiltration,
        slide.percent_granulocyte_infiltration,
    )
    utils.assert_float_equal(
        result_slide.percent_inflam_infiltration, slide.percent_inflam_infiltration
    )
    utils.assert_float_equal(
        result_slide.percent_lymphocyte_infiltration,
        slide.percent_lymphocyte_infiltration,
    )
    utils.assert_float_equal(
        result_slide.percent_monocyte_infiltration, slide.percent_monocyte_infiltration
    )
    utils.assert_float_equal(result_slide.percent_necrosis, slide.percent_necrosis)
    utils.assert_float_equal(
        result_slide.percent_neutrophil_infiltration,
        slide.percent_neutrophil_infiltration,
    )
    utils.assert_float_equal(
        result_slide.percent_normal_cells, slide.percent_normal_cells
    )
    utils.assert_float_equal(
        result_slide.percent_rhabdoid_features, slide.percent_rhabdoid_features
    )
    utils.assert_float_equal(
        result_slide.percent_sarcomatoid_features, slide.percent_sarcomatoid_features
    )
    utils.assert_float_equal(
        result_slide.percent_stromal_cells, slide.percent_stromal_cells
    )
    utils.assert_float_equal(
        result_slide.percent_tumor_cells, slide.percent_tumor_cells
    )
    utils.assert_float_equal(
        result_slide.percent_tumor_nuclei, slide.percent_tumor_nuclei
    )
    utils.assert_float_equal(
        result_slide.prostatic_chips_positive_count,
        slide.prostatic_chips_positive_count,
    )
    utils.assert_float_equal(
        result_slide.prostatic_chips_total_count, slide.prostatic_chips_total_count
    )
    utils.assert_float_equal(
        result_slide.prostatic_involvement_percent, slide.prostatic_involvement_percent
    )
    assert result_slide.section_location == slide.section_location
    assert result_slide.slide_id == slide.slide_id
    assert result_slide.submitter_id == slide.submitter_id
    assert (
        result_slide.tissue_microarray_coordinates
        == slide.tissue_microarray_coordinates
    )


def assert_portions_translated(
    result_portions: Iterable[sql.Row], portions: Iterable[Portion]
) -> None:
    result_portion = more_itertools.one(result_portions)
    portion = more_itertools.one(portions)

    utils.assert_float_equal(
        result_portion.creation_datetime, portion.creation_datetime
    )
    assert util.strtobool(result_portion.is_ffpe) == portion.is_ffpe
    assert result_portion.portion_id == portion.portion_id
    assert result_portion.portion_number == portion.portion_number
    assert result_portion.submitter_id == portion.submitter_id
    utils.assert_float_equal(result_portion.weight, portion.weight)

    assert_analytes_translated(result_portion.analytes, portion.analytes)
    assert_center_translated(result_portion.center, portion.center)
    assert_slides_translated(result_portion.slides, portion.slides)


def assert_samples_translated(
    result_samples: Iterable[sql.Row], samples: Iterable[Sample]
) -> None:
    result_sample = more_itertools.one(result_samples)
    sample = more_itertools.one(samples)

    assert result_sample.biospecimen_anatomic_site == sample.biospecimen_anatomic_site
    assert result_sample.biospecimen_laterality == sample.biospecimen_laterality
    assert result_sample.catalog_reference == sample.catalog_reference
    assert result_sample.composition == sample.composition
    utils.assert_float_equal(result_sample.current_weight, sample.current_weight)
    assert result_sample.days_to_collection == sample.days_to_collection
    assert result_sample.days_to_sample_procurement == sample.days_to_sample_procurement
    assert (
        result_sample.diagnosis_pathologically_confirmed
        == sample.diagnosis_pathologically_confirmed
    )
    assert result_sample.distance_normal_to_tumor == sample.distance_normal_to_tumor
    assert result_sample.distributor_reference == sample.distributor_reference
    assert result_sample.freezing_method == sample.freezing_method
    assert result_sample.growth_rate == sample.growth_rate
    utils.assert_float_equal(result_sample.initial_weight, sample.initial_weight)
    utils.assert_float_equal(
        result_sample.intermediate_dimension, sample.intermediate_dimension
    )
    assert util.strtobool(result_sample.is_ffpe) == sample.is_ffpe
    utils.assert_float_equal(result_sample.longest_dimension, sample.longest_dimension)
    assert (
        result_sample.method_of_sample_procurement
        == sample.method_of_sample_procurement
    )
    assert result_sample.oct_embedded == sample.oct_embedded
    assert result_sample.passage_count == sample.passage_count
    assert result_sample.pathology_report_uuid == sample.pathology_report_uuid
    assert result_sample.preservation_method == sample.preservation_method
    assert result_sample.sample_id == sample.sample_id
    assert result_sample.sample_ordinal == sample.sample_ordinal
    assert result_sample.sample_type == sample.sample_type
    assert result_sample.sample_type_id == sample.sample_type_id
    utils.assert_float_equal(
        result_sample.shortest_dimension, sample.shortest_dimension
    )
    assert result_sample.submitter_id == sample.submitter_id
    utils.assert_float_equal(
        result_sample.time_between_clamping_and_freezing,
        sample.time_between_clamping_and_freezing,
    )
    utils.assert_float_equal(
        result_sample.time_between_excision_and_freezing,
        sample.time_between_excision_and_freezing,
    )
    assert result_sample.tissue_collection_type == sample.tissue_collection_type
    assert result_sample.tissue_type == sample.tissue_type
    assert result_sample.tumor_code == sample.tumor_code
    assert result_sample.tumor_code_id == sample.tumor_code_id
    assert result_sample.tumor_descriptor == sample.tumor_descriptor

    assert_portions_translated(result_sample.portions, sample.portions)


def assert_hit_translated(result_case: sql.Row, hit: Hit) -> None:
    assert_samples_translated(result_case.samples, hit._source.samples)
