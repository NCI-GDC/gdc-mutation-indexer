import dataclasses
import enum
from typing import Sequence


class BackupMode(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    NEITHER = enum.auto()
    BOTH = enum.auto()

    def is_write(self) -> bool:
        return self == BackupMode.WRITE or self == BackupMode.BOTH

    def is_read(self) -> bool:
        return self == BackupMode.READ or self == BackupMode.BOTH


@dataclasses.dataclass(frozen=True)
class Backup:
    mode: BackupMode
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
