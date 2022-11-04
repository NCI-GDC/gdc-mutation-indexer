import yaml

import importlib_resources as resources
from pyspark.sql import types


def load_schema(schema_filename: str) -> types.StructType:
    schema = resources.files(__name__).joinpath(schema_filename).read_text()

    return types.StructType.fromJson(yaml.safe_load(schema))
