import enum


class IndexType(enum.IntEnum):
    CASE_CENTRIC = enum.auto()
    CNV_CENTRIC = enum.auto()
    CNV_OCCURRENCE_CENTRIC = enum.auto()
    GENE_CENTRIC = enum.auto()
    SSM_CENTRIC = enum.auto()
    SSM_OCCURRENCE_CENTRIC = enum.auto()
    GENE_EXPRESSION = enum.auto()


class BackupMode(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    NEITHER = enum.auto()
    BOTH = enum.auto()

    def is_write(self) -> bool:
        return self == BackupMode.WRITE or self == BackupMode.BOTH

    def is_read(self) -> bool:
        return self == BackupMode.READ or self == BackupMode.BOTH
