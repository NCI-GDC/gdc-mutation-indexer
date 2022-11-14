import decimal
import json
import uuid
from os import path
from typing import Any, Optional

from pyspark.sql import types

DECIMAL_CONTEXT = decimal.Context(prec=10)


def load_schema(schema_dir: str, file_name: str) -> types.StructType:
    file_name = path.join(schema_dir, file_name)

    with open(file_name, "r") as f:
        return types.StructType.fromJson(json.load(f))


def generate_uuid5(*args: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(arg) for arg in args)))


def assert_float_equal(a: Optional[float], b: Optional[float]) -> None:
    decimal_a = a if a is None else DECIMAL_CONTEXT.create_decimal_from_float(a)
    decimal_b = b if b is None else DECIMAL_CONTEXT.create_decimal_from_float(b)

    assert decimal_a == decimal_b
