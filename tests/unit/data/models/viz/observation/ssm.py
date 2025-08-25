import dataclasses


@dataclasses.dataclass(frozen=True)
class SSM:
    @dataclasses.dataclass(frozen=True)
    class Observation:
        @dataclasses.dataclass(frozen=True)
        class InputBAMFile:
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
        class Sample:
            matched_norm_sample_barcode: str | None = "MBCProject_3808_SALIVA_1"
            matched_norm_sample_uuid: None | (str) = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
            tumor_sample_barcode: str | None = "MBCProject_3808_T1_WES_1"
            tumor_sample_uuid: str | None = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"

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
            variant_caller: str | None = "muse;varscan2"
            variant_process: str | None = "masked"

        center: str | None = "BI"
        input_bam_file: InputBAMFile | None = InputBAMFile()
        mutation_status: str | None = "Somatic"
        normal_genotype: NormalGenotype | None = NormalGenotype()
        observation_id: str | None = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
        read_depth: ReadDepth | None = ReadDepth()
        sample: Sample | None = Sample()
        tumor_genotype: TumorGenotype | None = TumorGenotype()
        validation: Validation | None = Validation()
        variant_calling: VariantCalling | None = VariantCalling()

    ssm_id: str | None = "ssm-0"
    case_id: str | None = "case-0"
    occurrence_id: str | None = "1746a06f-2052-5fec-8150-8da04c939ee4"
    observation: tuple[Observation, ...] | None = (Observation(),)
