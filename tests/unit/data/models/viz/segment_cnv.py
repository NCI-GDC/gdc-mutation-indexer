import dataclasses


@dataclasses.dataclass(frozen=True)
class SegmentCNV:
    segment_cnv_id: str = "segment_cnv-0"
    occurrence_id: str = "occ-0"
    observation_id: str = "obs-0"
    case_id: str = "case-0"
    aliquot_id: str = "aliquot-0"
    src_file_id: str = "file-0"
    chromosome: str = "chr1"
    variant_caller: str = "AscatNGS"
    variant_status: str = "Tumor Only"
    length: int = 51
    start_position: int = 25
    end_position: int = 75
    cnv_change: str = "Gain"
    cnv_change_5_category: str = "Amplification"
    sample_ploidy_integer: int = 5
    copy_number: int = 10
