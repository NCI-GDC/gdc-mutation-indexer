import dataclasses


@dataclasses.dataclass(frozen=True)
class CNV:
    @dataclasses.dataclass(frozen=True)
    class Observation:
        @dataclasses.dataclass(frozen=True)
        class VariantCalling:
            variant_caller: str | None = "ASCAT"

        copy_number: int | None = 5
        observation_id: str | None = "obs-0"
        sample_ploidy_integer: int | None = 2
        src_file_id: str | None = "file-0"
        variant_calling: VariantCalling | None = VariantCalling()
        variant_status: str | None = "Tumor Only"

    cnv_id: str | None = "cnv-0"
    case_id: str | None = "case-0"
    occurrence_id: str | None = "occ-0"
    observation: tuple[Observation, ...] | None = (Observation(),)
