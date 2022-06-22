from importlib import resources

import yaml
from pyspark.sql import types


def load_schema(schema_filename: str) -> types.StructType:
    with resources.path(__name__, "") as p:
        schema = p.joinpath(schema_filename).read_text()
    
    return types.StructType.fromJson(yaml.safe_load(schema))
