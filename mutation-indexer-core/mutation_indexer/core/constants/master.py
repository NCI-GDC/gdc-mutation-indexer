import enum


class Driver(enum.Enum):
    VIZ = enum.auto()
    GENE_EXPRESSION = enum.auto()

    @property
    def module_name(self) -> str:
        return self.name.lower()
