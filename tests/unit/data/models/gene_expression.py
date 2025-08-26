import dataclasses
import datetime

from tests.unit.data.models.builders import GeneModel

__all__ = ("ExpressionValue", "File", "GeneModel", "PrimaryAliquot", "STARCounts")


@dataclasses.dataclass(frozen=True)
class File:
    @dataclasses.dataclass(frozen=True)
    class Case:
        @dataclasses.dataclass(frozen=True)
        class Sample:
            sample_id: str = "sample-0"
            sample_type: str = "tumor"

        case_id: str = "case-0"
        submitter_id: str = "sub-id"
        samples: tuple[Sample, ...] = (Sample(),)

    file_id: str = "file-0"
    created_datetime: str = datetime.datetime.min.isoformat(timespec="microseconds")
    experimental_strategy: str = "WXS"
    cases: tuple[Case, ...] = (Case(),)


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    file_id: str = "file-0"
    case_id: str = "case-0"
    submitter_id: str = "case 0"


@dataclasses.dataclass(frozen=True)
class STARCounts:
    did: str = "file-0"
    gene_id: str = "ENSG00000238009"
    gene_name: str = "STAR-GENE"
    gene_type: str = "protein_coding"
    unstranded: int = 2
    stranded_first: int = 123
    stranded_second: int = 393
    tpm_unstranded: float = 22.10
    fpkm_unstranded: float = 33902.3
    fpkm_uq_unstranded: float = 22901.8


@dataclasses.dataclass(frozen=True)
class ExpressionValue:
    case_id: str = "case-0"
    gene_expression_id: str = "ge-0"
    gene_id: str = "ENSG00000238009"
    log2_uqfpkm: float = 342.293
    submitter_id: str = "sub-case-0"
    symbol: str = "STAR-GENE"
    uqfpkm: float = 432.903
