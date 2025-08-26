import dataclasses


@dataclasses.dataclass(frozen=True)
class ASCATMetadata:
    aliquot_id: str | None = "aliquot-0"
    case_id: str | None = "case-0"
    file_id: str | None = "file-0"
    workflow_type: str | None = "Pindel Annotation"
    analysis_id: str | None = "analysis-0"
