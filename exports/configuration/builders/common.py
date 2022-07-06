import dataclasses
from typing import Sequence

from exports.constants import build


@dataclasses.dataclass(frozen=True)
class Backup:
    mode: build.BackupMode
    path: str


@dataclasses.dataclass(frozen=True)
class Builder:
    is_cached: bool
    backup: Backup
    projects: Sequence[str] = dataclasses.field(metadata={"load_only": True})


@dataclasses.dataclass(frozen=True)
class CentricBuilder(Builder):
    repartition_size: int
    coalesce_size: int


@dataclasses.dataclass(frozen=True)
class GeneModelBuilder(Builder):
    census_file: str
    citobands_file: str
    gene_model_file: str
