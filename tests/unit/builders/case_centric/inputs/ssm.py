import dataclasses
from typing import Optional, Tuple

import more_itertools
from pyspark import sql

from tests.unit import utils


@dataclasses.dataclass(frozen=True)
class InputBamFile:
    normal_bam_uuid: Optional[str] = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    tumor_bam_uuid: Optional[str] = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"


@dataclasses.dataclass(frozen=True)
class NormalGenotype:
    match_norm_seq_allele1: Optional[str] = None
    match_norm_seq_allele2: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class ReadDepth:
    n_depth: Optional[int] = 38
    t_alt_count: Optional[int] = 5
    t_depth: Optional[int] = 29
    t_ref_count: Optional[int] = 24


@dataclasses.dataclass(frozen=True)
class TumorGenotype:
    tumor_seq_allele1: Optional[str] = "C"
    tumor_seq_allele2: Optional[str] = "A"


@dataclasses.dataclass(frozen=True)
class Validation:
    tumor_validation_allele1: Optional[str] = None
    tumor_validation_allele2: Optional[str] = None
    validation_method: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class VariantCalling:
    variant_caller: Optional[str] = "muse"
    variant_process: Optional[str] = "masked"


@dataclasses.dataclass(frozen=True)
class Observation:
    center: Optional[str] = "BI"
    input_bam_file: InputBamFile = InputBamFile()
    mutation_status: Optional[str] = "Somatic"
    normal_genotype: NormalGenotype = NormalGenotype()
    observation_id: Optional[str] = "f64f0e40-4620-5435-ad1e-38a63faf4e94"
    read_depth: ReadDepth = ReadDepth()
    tumor_genotype: TumorGenotype = TumorGenotype()
    validation: Validation = Validation()
    variant_calling: VariantCalling = VariantCalling()


@dataclasses.dataclass(frozen=True)
class Observations:
    case_id: Optional[str] = "case-0"
    observation: Tuple[Observation, ...] = (Observation(),)
    occurrence_id: Optional[str] = "occ-0"
    ssm_id: Optional[str] = "ssm-0"


@dataclasses.dataclass(frozen=True)
class Annotation:
    amino_acids: Optional[str] = None
    ccds: Optional[str] = "CCDS380.1"
    cdna_position: Optional[str] = None
    cds_end: Optional[int] = None
    cds_length: Optional[int] = None
    cds_position: Optional[int] = None
    cds_start: Optional[int] = None
    clin_sig: Optional[str] = None
    codons: Optional[str] = None
    dbsnp_rs: Optional[str] = "novel"
    dbsnp_val_status: Optional[str] = None
    domains: Optional[str] = None
    ensp: Optional[str] = None
    existing_variation: Optional[str] = None
    hgvsc: Optional[str] = "c.1705G>T"
    hgvsp: Optional[str] = None
    hgvsp_short: Optional[str] = None
    polyphen_impact: Optional[str] = "benign"
    polyphen_score: Optional[float] = 0.305
    protein_position: Optional[str] = None
    pubmed: Optional[str] = None
    sift_impact: Optional[str] = "tolerated"
    sift_score: Optional[float] = 0.12
    swissprot: Optional[str] = None
    transcript_id: Optional[str] = "ENST00000373388"
    trembl: Optional[str] = None
    uniparc: Optional[str] = None
    vep_impact: Optional[str] = "MODERATE"


@dataclasses.dataclass(frozen=True)
class Transcript:
    aa_change: Optional[str] = "A569S"
    annotation: Annotation = Annotation()
    consequence_type: Optional[str] = "missense_variant"
    ref_seq_accession: Optional[str] = "NM_052896.4"
    transcript_id: Optional[str] = "ENST00000373388"


@dataclasses.dataclass(frozen=True)
class Consequence:
    consequence_id: Optional[str] = "con-0"
    transcript: Transcript = Transcript()


@dataclasses.dataclass(frozen=True)
class Consequences:
    consequence: Tuple[Consequence, ...] = (Consequence(),)
    ssm_id: Optional[str] = "ssm-0"


def assert_annotation_translated(
    result_annotation: sql.Row, annotation: Annotation
) -> None:
    assert result_annotation.amino_acids == annotation.amino_acids
    assert result_annotation.ccds == annotation.ccds
    assert result_annotation.cdna_position == annotation.cdna_position
    assert result_annotation.cds_end == annotation.cds_end
    assert result_annotation.cds_length == annotation.cds_length
    assert result_annotation.cds_position == annotation.cds_position
    assert result_annotation.cds_start == annotation.cds_start
    assert result_annotation.clin_sig == annotation.clin_sig
    assert result_annotation.codons == annotation.codons
    assert result_annotation.dbsnp_rs == annotation.dbsnp_rs
    assert result_annotation.dbsnp_val_status == annotation.dbsnp_val_status
    assert result_annotation.domains == annotation.domains
    assert result_annotation.ensp == annotation.ensp
    assert result_annotation.existing_variation == annotation.existing_variation
    assert result_annotation.hgvsc == annotation.hgvsc
    assert result_annotation.hgvsp == annotation.hgvsp
    assert result_annotation.hgvsp_short == annotation.hgvsp_short
    assert result_annotation.polyphen_impact == annotation.polyphen_impact
    utils.assert_float_equal(
        result_annotation.polyphen_score, annotation.polyphen_score
    )
    assert result_annotation.protein_position == annotation.protein_position
    assert result_annotation.pubmed == annotation.pubmed
    assert result_annotation.sift_impact == annotation.sift_impact
    utils.assert_float_equal(result_annotation.sift_score, annotation.sift_score)
    assert result_annotation.swissprot == annotation.swissprot
    assert result_annotation.transcript_id == annotation.transcript_id
    assert result_annotation.trembl == annotation.trembl
    assert result_annotation.uniparc == annotation.uniparc
    assert result_annotation.vep_impact == annotation.vep_impact


def assert_transcript_translated(
    result_transcript: sql.Row, transcript: Transcript
) -> None:
    assert result_transcript.transcript_id == transcript.transcript_id
    assert result_transcript.aa_change == transcript.aa_change
    assert result_transcript.consequence_type == transcript.consequence_type
    assert result_transcript.ref_seq_accession == transcript.ref_seq_accession

    assert_annotation_translated(result_transcript.annotation, transcript.annotation)


def assert_consequences_translated(
    result_gene: sql.Row, consequences: Consequences
) -> None:
    result_ssm = more_itertools.one(result_gene.ssm)
    result_consequence = more_itertools.one(result_ssm.consequence)
    consequence = more_itertools.one(consequences.consequence)

    assert result_consequence.consequence_id == consequence.consequence_id

    assert_transcript_translated(result_consequence.transcript, consequence.transcript)


def assert_input_bam_file_translated(
    result_input_bam_file: sql.Row, input_bam_file: InputBamFile
) -> None:
    assert result_input_bam_file.normal_bam_uuid == input_bam_file.normal_bam_uuid
    assert result_input_bam_file.tumor_bam_uuid == input_bam_file.tumor_bam_uuid


def assert_normal_genotype_translated(
    result_normal_genotype: sql.Row, normal_genotype: NormalGenotype
) -> None:
    assert (
        result_normal_genotype.match_norm_seq_allele1
        == normal_genotype.match_norm_seq_allele1
    )
    assert (
        result_normal_genotype.match_norm_seq_allele2
        == normal_genotype.match_norm_seq_allele2
    )


def assert_read_depth_translated(
    result_read_depth: sql.Row, read_depth: ReadDepth
) -> None:
    assert result_read_depth.n_depth == read_depth.n_depth
    assert result_read_depth.t_alt_count == read_depth.t_alt_count
    assert result_read_depth.t_depth == read_depth.t_depth
    assert result_read_depth.t_ref_count == read_depth.t_ref_count


def assert_tumor_genotype_translated(
    result_tumor_genotype: sql.Row, tumor_genotype: TumorGenotype
) -> None:
    assert result_tumor_genotype.tumor_seq_allele1 == tumor_genotype.tumor_seq_allele1
    assert result_tumor_genotype.tumor_seq_allele2 == tumor_genotype.tumor_seq_allele2


def assert_validation_translated(
    result_validation: sql.Row, validation: Validation
) -> None:
    assert (
        result_validation.tumor_validation_allele1
        == validation.tumor_validation_allele1
    )
    assert (
        result_validation.tumor_validation_allele2
        == validation.tumor_validation_allele2
    )
    assert result_validation.validation_method == validation.validation_method


def assert_variant_calling_translated(
    result_variant_calling: sql.Row, variant_calling: VariantCalling
) -> None:
    assert result_variant_calling.variant_caller == variant_calling.variant_caller
    assert result_variant_calling.variant_process == variant_calling.variant_process


def assert_observation_transated(
    result_gene: sql.Row, observations: Observations
) -> None:
    result_ssm = more_itertools.one(result_gene.ssm)
    result_observation = more_itertools.one(result_ssm.observation)
    observation = more_itertools.one(observations.observation)

    assert result_observation.center == observation.center
    assert result_observation.mutation_status == observation.mutation_status
    assert result_observation.observation_id == observation.observation_id

    assert_input_bam_file_translated(
        result_observation.input_bam_file, observation.input_bam_file
    )
    assert_normal_genotype_translated(
        result_observation.normal_genotype, observation.normal_genotype
    )
    assert_read_depth_translated(result_observation.read_depth, observation.read_depth)
    assert_tumor_genotype_translated(
        result_observation.tumor_genotype, observation.tumor_genotype
    )
    assert_validation_translated(result_observation.validation, observation.validation)
    assert_variant_calling_translated(
        result_observation.variant_calling, observation.variant_calling
    )
