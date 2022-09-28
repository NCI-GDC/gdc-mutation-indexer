import enum
import pathlib

import importlib_resources as resources
import yaml
from pyspark.sql import types


def load_schema(schema_filename: str) -> types.StructType:
    schema = resources.files(__name__).joinpath(schema_filename).read_text()

    return types.StructType.fromJson(yaml.safe_load(schema))


class Schema(enum.Enum):
    @property
    def schema_dir(self) -> pathlib.Path:
        raise NotImplementedError("Each subclass must override.")

    def load_schema(self) -> types.StructType:
        return load_schema(self.schema_dir.joinpath(self.value))
