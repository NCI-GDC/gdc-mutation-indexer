import abc
import enum
from typing import Optional

from typing_extensions import Self

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s"


class DataFrame(enum.Enum):
    def to_param(self) -> str:
        return f"{self.name}_df".lower()
    
    @classmethod
    def from_param(cls: type[Self], param: str) -> Self:
        return cls[param[:-3].upper()]



class IndexType(enum.Enum, abc.ABC):
    @abc.abstractmethod
    def get_mappings_details(self) -> tuple[str, Optional[str]]:
        pass


class BackupMode(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    NEITHER = enum.auto()
    BOTH = enum.auto()

    def is_write(self) -> bool:
        return self == BackupMode.WRITE or self == BackupMode.BOTH

    def is_read(self) -> bool:
        return self == BackupMode.READ or self == BackupMode.BOTH
