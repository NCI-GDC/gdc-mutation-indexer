import json

import pkg_resources
from pyspark.sql import types


def load_schema(schema_filename: str) -> types.StructType:
    with pkg_resources.resource_stream("exports.schemas", schema_filename) as f:
        return types.StructType.fromJson(json.load(f))
