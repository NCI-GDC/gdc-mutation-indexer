import enum
from typing import Optional

from mutation_indexer import constants


class DataFrame(constants.DataFrame, enum.IntEnum):
    ASCAT = enum.auto()
    CASE = enum.auto()
    GENE_MODEL = enum.auto()
    MAF = enum.auto()
    MAF_METADATA = enum.auto()
    PRIMARY_ALIQUOT = enum.auto()

    def to_param(self) -> str:
        return f"{self.name}_df".lower()

    @classmethod
    def from_param(cls, param: str) -> "DataFrame":
        return cls[param[:-3].upper()]


class IndexType(constants.IndexType, enum.IntEnum):
    FILE = enum.auto()
    CASE = enum.auto()
    CASE_CENTRIC = enum.auto()
    CNV_CENTRIC = enum.auto()
    CNV_OCCURRENCE_CENTRIC = enum.auto()
    GENE_CENTRIC = enum.auto()
    SSM_CENTRIC = enum.auto()
    SSM_OCCURRENCE_CENTRIC = enum.auto()

    def get_mappings_details(self) -> tuple[str, Optional[str]]:
        if self in (IndexType.CASE, IndexType.FILE):
            return "gdc_from_graph", self.name.lower()

        return self.name.lower(), None
