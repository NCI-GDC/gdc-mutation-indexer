import json
import uuid
from os import path
from typing import Any

from pyspark.sql import types


def load_schema(schema_dir: str, file_name: str) -> types.StructType:
    file_name = path.join(schema_dir, file_name)

    with open(file_name, "r") as f:
        return types.StructType.fromJson(json.load(f))


def generate_uuid5(*args: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(arg) for arg in args)))
