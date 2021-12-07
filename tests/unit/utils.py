import json
from os import path

from pyspark.sql import types


def load_schema(schema_dir: str, file_name: str) -> types.StructType:
    file_name = path.join(schema_dir, file_name)

    with open(file_name, "r") as f:
        return types.StructType.fromJson(json.load(f))
