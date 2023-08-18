import enum
from typing import Optional

from mutation_indexer import constants


class DataFrame(constants.DataFrame, enum.IntEnum):
    CASE = enum.auto()
    EXPRESSION_VALUE = enum.auto()
    GENE_EXPRESSION = enum.auto()
    GENE_MODEL = enum.auto()
    PRIMARY_ALIQUOT = enum.auto()


class IndexType(constants.IndexType, enum.IntEnum):
    FILE = enum.auto()
    GENE_EXPRESSION = enum.auto()

    def get_mappings_details(self) -> tuple[str, Optional[str]]:
        if self == IndexType.FILE:
            return "gdc_from_graph", self.name.lower()

        return self.name.lower(), None
