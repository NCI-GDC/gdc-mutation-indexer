import dataclasses


@dataclasses.dataclass(frozen=True)
class MAFMetadata:
    case_id: str = "case-0"
    data_type: str = "MAF"
    file_id: str = "file-0"
    workflow: str = "MAF Workflow"
