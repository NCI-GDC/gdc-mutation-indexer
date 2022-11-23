import abc
import enum
from os import path

import importlib_resources as resources
import yaml
from pyspark.sql import types


class Schema(enum.Enum):
    @property
    @abc.abstractmethod
    def _prefix(self) -> str:
        pass

    def filename(self) -> str:
        return path.join(self._prefix, self.value)

    def load(self) -> types.StructType:
        schema_data = resources.files(__name__).joinpath(self.filename()).read_text()

        return types.StructType.fromJson(yaml.safe_load(schema_data))
