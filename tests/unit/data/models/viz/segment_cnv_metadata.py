import dataclasses


@dataclasses.dataclass(frozen=True)
class SegmentCNVMetadata:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    workflow_type: str = "AscatNGS"
    analysis_id: str = "analysis-0"
