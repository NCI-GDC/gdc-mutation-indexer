import io
from collections.abc import Iterable
from typing import Optional, cast

import yaml
from pyspark.sql import types

SPARK_TYPES = {
    "keyword": types.StringType(),
    "text": types.StringType(),
    "boolean": types.BooleanType(),
    "double": types.DoubleType(),
    "float": types.FloatType(),
    "long": types.LongType(),
}


def _get_fields(properties: dict[str, dict]) -> Iterable[types.StructField]:
    for prop, details in properties.items():
        prop_type = cast(Optional[str], details.get("type"))

        if prop_type in SPARK_TYPES:
            yield types.StructField(prop, SPARK_TYPES[prop_type])
        elif prop_type == "nested":
            yield types.StructField(prop, types.ArrayType(_to_schema(details)))
        elif "properties" in details:
            yield types.StructField(prop, _to_schema(details))
        else:
            raise ValueError(
                f"Unknown property type in the mapping encountered for {prop}: {prop_type}."
            )


def _to_schema(mapping: dict) -> types.StructType:
    return types.StructType(fields=list(_get_fields(mapping["properties"])))


def translate(
    mapping_file: io.TextIOBase, output_file: io.TextIOBase, included_properties: str
) -> None:
    properties_set = frozenset(included_properties.split(","))
    mapping = yaml.safe_load(mapping_file)
    mapping["properties"] = {
        p: d for p, d in mapping.get("properties", {}).items() if p in properties_set
    }

    yaml.safe_dump(_to_schema(mapping).jsonValue(), output_file)
