import dataclasses

import more_itertools
from pyspark import sql

from tests.unit import utils


@dataclasses.dataclass(frozen=True)
class InputBamFile:
    normal_bam_uuid: str | None = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    tumor_bam_uuid: str | None = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"


@dataclasses.dataclass(frozen=True)
class NormalGenotype:
    match_norm_seq_allele1: str | None = None
    match_norm_seq_allele2: str | None = None


@dataclasses.dataclass(frozen=True)
class ReadDepth:
    n_depth: int | None = 38
    t_alt_count: int | None = 5
    t_depth: int | None = 29
    t_ref_count: int | None = 24


@dataclasses.dataclass(frozen=True)
class TumorGenotype:
    tumor_seq_allele1: str | None = "C"
    tumor_seq_allele2: str | None = "A"


@dataclasses.dataclass(frozen=True)
class Validation:
    tumor_validation_allele1: str | None = None
    tumor_validation_allele2: str | None = None
    validation_method: str | None = None


@dataclasses.dataclass(frozen=True)
class VariantCalling:
    variant_caller: str | None = "muse"
    variant_process: str | None = "masked"


@dataclasses.dataclass(frozen=True)
class Observation:
    center: str | None = "BI"
    input_bam_file: InputBamFile = InputBamFile()
    mutation_status: str | None = "Somatic"
    normal_genotype: NormalGenotype = NormalGenotype()
    observation_id: str | None = "f64f0e40-4620-5435-ad1e-38a63faf4e94"
    read_depth: ReadDepth = ReadDepth()
    tumor_genotype: TumorGenotype = TumorGenotype()
    validation: Validation = Validation()
    variant_calling: VariantCalling = VariantCalling()


@dataclasses.dataclass(frozen=True)
class Observations:
    case_id: str | None = "case-0"
    observation: tuple[Observation, ...] = (Observation(),)
    occurrence_id: str | None = "occ-0"
    ssm_id: str | None = "ssm-0"


@dataclasses.dataclass(frozen=True)
class Annotation:
    amino_acids: str | None = None
    ccds: str | None = "CCDS380.1"
    cdna_position: str | None = None
    cds_end: int | None = None
    cds_length: int | None = None
    cds_position: int | None = None
    cds_start: int | None = None
    clin_sig: str | None = None
    codons: str | None = None
    dbsnp_rs: str | None = "novel"
    dbsnp_val_status: str | None = None
    domains: str | None = None
    ensp: str | None = None
    existing_variation: str | None = None
    hgvsc: str | None = "c.1705G>T"
    hgvsp: str | None = None
    hgvsp_short: str | None = None
    polyphen_impact: str | None = "benign"
    polyphen_score: float | None = 0.305
    protein_position: str | None = None
    pubmed: str | None = None
    sift_impact: str | None = "tolerated"
    sift_score: float | None = 0.12
    swissprot: str | None = None
    transcript_id: str | None = "ENST00000373388"
    trembl: str | None = None
    uniparc: str | None = None
    vep_impact: str | None = "MODERATE"


@dataclasses.dataclass(frozen=True)
class Transcript:
    aa_change: str | None = "A569S"
    annotation: Annotation = Annotation()
    consequence_type: str | None = "missense_variant"
    ref_seq_accession: str | None = "NM_052896.4"
    transcript_id: str | None = "ENST00000373388"


@dataclasses.dataclass(frozen=True)
class Consequence:
    consequence_id: str | None = "con-0"
    transcript: Transcript = Transcript()


@dataclasses.dataclass(frozen=True)
class Consequences:
    consequence: tuple[Consequence, ...] = (Consequence(),)
    ssm_id: str | None = "ssm-0"


def assert_annotation_translated(result_annotation: sql.Row, annotation: Annotation) -> None:
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
    utils.assert_float_equal(result_annotation.polyphen_score, annotation.polyphen_score)
    assert result_annotation.protein_position == annotation.protein_position
    assert result_annotation.pubmed == annotation.pubmed
    assert result_annotation.sift_impact == annotation.sift_impact
    utils.assert_float_equal(result_annotation.sift_score, annotation.sift_score)
    assert result_annotation.swissprot == annotation.swissprot
    assert result_annotation.transcript_id == annotation.transcript_id
    assert result_annotation.trembl == annotation.trembl
    assert result_annotation.uniparc == annotation.uniparc
    assert result_annotation.vep_impact == annotation.vep_impact


def assert_transcript_translated(result_transcript: sql.Row, transcript: Transcript) -> None:
    assert result_transcript.transcript_id == transcript.transcript_id
    assert result_transcript.aa_change == transcript.aa_change
    assert result_transcript.consequence_type == transcript.consequence_type
    assert result_transcript.ref_seq_accession == transcript.ref_seq_accession

    assert_annotation_translated(result_transcript.annotation, transcript.annotation)


def assert_consequences_translated(result_gene: sql.Row, consequences: Consequences) -> None:
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
        result_normal_genotype.match_norm_seq_allele1 == normal_genotype.match_norm_seq_allele1
    )
    assert (
        result_normal_genotype.match_norm_seq_allele2 == normal_genotype.match_norm_seq_allele2
    )


def assert_read_depth_translated(result_read_depth: sql.Row, read_depth: ReadDepth) -> None:
    assert result_read_depth.n_depth == read_depth.n_depth
    assert result_read_depth.t_alt_count == read_depth.t_alt_count
    assert result_read_depth.t_depth == read_depth.t_depth
    assert result_read_depth.t_ref_count == read_depth.t_ref_count


def assert_tumor_genotype_translated(
    result_tumor_genotype: sql.Row, tumor_genotype: TumorGenotype
) -> None:
    assert result_tumor_genotype.tumor_seq_allele1 == tumor_genotype.tumor_seq_allele1
    assert result_tumor_genotype.tumor_seq_allele2 == tumor_genotype.tumor_seq_allele2


def assert_validation_translated(result_validation: sql.Row, validation: Validation) -> None:
    assert result_validation.tumor_validation_allele1 == validation.tumor_validation_allele1
    assert result_validation.tumor_validation_allele2 == validation.tumor_validation_allele2
    assert result_validation.validation_method == validation.validation_method


def assert_variant_calling_translated(
    result_variant_calling: sql.Row, variant_calling: VariantCalling
) -> None:
    assert result_variant_calling.variant_caller == variant_calling.variant_caller
    assert result_variant_calling.variant_process == variant_calling.variant_process


def assert_observation_translated(result_gene: sql.Row, observations: Observations) -> None:
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
