import dataclasses
from typing import Tuple

from tests.unit.data.models.builders import *


@dataclasses.dataclass(frozen=True)
class Demographic:
    days_to_death: int = 20
    ethnicity: str = "non-hispanic"
    gender: str = "female"
    race: str = "first nations"
    vital_status: str = "dead"


@dataclasses.dataclass(frozen=True)
class Project:
    project_id: str = "GDC-TEST"


@dataclasses.dataclass(frozen=True)
class Diagnosis:
    age_at_diagnosis: int = 74


@dataclasses.dataclass(frozen=True)
class Sample:
    sample_id: str = "sample-0"
    sample_type: str = "tumor"


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    file_id: str = "file-0"
    case_id: str = "case-0"
    submitter_id: str = "case 0"
    demographic: Demographic = Demographic()
    project: Project = Project()
    diagnoses: Tuple[Diagnosis, ...] = (Diagnosis(),)
    samples: Tuple[Sample, ...] = (Sample(),)


@dataclasses.dataclass(frozen=True)
class Case:
    case_id: str = "case-0"
    days_to_death: int = 38
    ethnicity: str = "hispanic"
    gender: str = "male"
    race: str = "indigenous"
    vital_status: str = "status"
    submitter_id: str = "sub-id"
    project_id: str = "GDC-TEST"
    file_id: str = "file-0"
    age_at_diagnosis: Tuple[int, ...] = (12,)


@dataclasses.dataclass(frozen=True)
class ValueGene:
    gene_id: str = "gene-0"
    expression_value: float = 328382.4458
    symbol: str = "genSym"


@dataclasses.dataclass(frozen=True)
class Value:
    file_id: str = "file-0"
    genes: Tuple[ValueGene, ...] = (ValueGene(),)
