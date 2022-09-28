import pathlib
from typing import Union

import importlib_resources as resources
import yaml
from pyspark.sql import types

from exports import schemas


def load_schema(schema_filename: Union[str, pathlib.Path]) -> types.StructType:
    schema = resources.files(__name__).joinpath(schema_filename).read_text()

    return types.StructType.fromJson(yaml.safe_load(schema))


class Schema(schemas.Schema):
    def load_schema(self) -> types.StructType:
        return load_schema(self.schema_dir.joinpath(self.value))
