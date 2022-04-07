import importlib_resources as resources
import yaml
from pyspark.sql import types


def load_schema(schema_filename: str) -> types.StructType:
    filename = resources.files(__name__).joinpath(schema_filename)

    with open(filename) as f:
        return types.StructType.fromJson(yaml.safe_load(f))
