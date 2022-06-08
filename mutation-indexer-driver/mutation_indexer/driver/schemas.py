from types import ModuleType
from typing import Union

import importlib_resources as resources
import yaml
from pyspark.sql import types


def load_schema(
    package: Union[ModuleType, str], schema_filename: str
) -> types.StructType:
    schema = resources.files(package).joinpath(schema_filename).read_text()

    return types.StructType.fromJson(yaml.safe_load(schema))
